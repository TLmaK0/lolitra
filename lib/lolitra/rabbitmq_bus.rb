module Lolitra
  class RabbitmqBus
    attr_accessor :connection
    attr_accessor :exchange
    attr_accessor :exchange_dead_letter
    attr_accessor :options
    attr_accessor :subscribers

    SUBSCRIBE_OPTIONS = {:durable => true}

    def initialize(hash = {})
      Lolitra::MessageHandlerManager.bus = self

      self.options = {
        :queue_prefix => "",
        :queue_suffix => "",
        :exchange_dead_suffix => ".dead",
        :exchange_dead_params => {}, 
        :queue_params => {},
        :queue_dead_suffix => ".dead",
        :queue_dead_params => {},
        :no_consume => false,
      }.merge(hash.delete(:options) || {})

      self.options[:queue_params][:arguments] = {} unless self.options[:queue_params][:arguments]

      self.options[:queue_params][:arguments] = {
        "x-dead-letter-exchange" => "#{hash[:exchange]}#{@options[:exchange_dead_suffix]}"
      }.merge(self.options[:queue_params][:arguments])

      @channels = {}
      @params = hash.reject { |key, value| !value }
      raise "no :exchange specified" unless hash[:exchange]

      AMQP::Utilities::EventLoopHelper.run do
        self.connection = AMQP.start(@params) do |connection|
          Lolitra::logger.info("Connected to rabbitmq.")
          channel = create_channel(connection) do |channel|
            begin
              self.exchange = channel.topic(@params[:exchange], :durable => true)
              self.exchange_dead_letter = channel.topic("#{@params[:exchange]}#{@options[:exchange_dead_suffix]}", :durable => true)

              @params[:subscribers].each do |handler|
                Lolitra::MessageHandlerManager.register_subscriber(handler)
              end
            rescue => e
              Lolitra::log_exception(e)
            end
          end
        end
        self.connection.on_tcp_connection_loss do |connection, settings|
          # reconnect in 10 seconds, without enforcement
          Lolitra::logger.info("Connection loss. Trying to reconnect in 10 secs if needed.")
          connection.reconnect(false, 10)
        end
      end
    end

    def disconnect(&block)
      self.connection.close(&block)
    end

    def subscribe(message_class, handler_class)
      create_queue(message_class, handler_class, SUBSCRIBE_OPTIONS)
    end

    def publish(message)
      #TODO: if exchange channel is closed doesn't log anything
      self.exchange.publish(message.marshall, :routing_key => message.class.message_key, :timestamp => Time.now.to_i)
    end

    def unsubscribe(handler_class, &block)
      queue_name = generate_queue_name(handler_class)
      begin
        create_channel(self.connection) do |channel|
          queue = channel.queue(queue_name, SUBSCRIBE_OPTIONS) do |queue|
            begin
              queue.delete do
                block.call(handler_class, true)
              end
            rescue => e
              Lolitra::log_exception(e)
              block.call(handler_class, false)
            end
          end
        end
      rescue => e
        Lolitra::log_exception(e)
      end
    end

    def process_deadletters(handler_class)
      queue_name_dead = generate_queue_name_dead(handler_class)
      options = SUBSCRIBE_OPTIONS 
      create_channel(self.connection) do |channel|
        begin
          channel.queue(queue_name_dead, options.merge(@options[:queue_dead_params])) do |queue|
            recursive_pop(channel, queue, handler_class)
          end
        rescue => e
          Lolitra::log_exception(e)
        end
      end
      true
    end

    def purge_deadletters(handler_class)
      queue_name_dead = generate_queue_name_dead(handler_class)
      options = SUBSCRIBE_OPTIONS 
      create_channel(self.connection) do |channel|
        begin
          channel.queue(queue_name_dead, options.merge(@options[:queue_dead_params])) do |queue|
            purge_queue(queue)
          end
        rescue => e
          Lolitra::log_exception(e)
        end
      end
      true
    end

    def remove_next_deadletter(handler_class)
      queue_name_dead = generate_queue_name_dead(handler_class)
      options = SUBSCRIBE_OPTIONS 
      create_channel(self.connection) do |channel|
        begin
          channel.queue(queue_name_dead, options.merge(@options[:queue_dead_params])) do |queue|
            queue.pop
          end
        rescue => e
          Lolitra::log_exception(e)
        end
      end
      true
    end

  private
    def purge_queue(queue)
      queue.pop do |info, payload|
        if (payload)
          purge_queue(queue)
        end
      end
    end

    def recursive_pop(channel, queue, handler_class)
      queue.pop(:ack => true) do |info, payload|
        if payload
          Lolitra::logger.info("Routing key: #{info.routing_key}")
          Lolitra::logger.info("Payload: #{payload}")
          begin
            message_class_tmp = handler_class.handlers[info.routing_key][0]
            handler_class.handle(message_class_tmp.unmarshall(payload))
            info.ack
            recursive_pop(channel, queue, handler_class)
          rescue => e
            channel.reject(info.delivery_tag, true)
            Lolitra::log_exception(e)
          end
        end
      end 
    end

    def publish_payload(routing_key, payload)
      self.exchange.publish(payload, :routing_key => routing_key, :timestamp => Time.now.to_i)
    end

    def create_channel(connection, &block)
      channel = AMQP::Channel.new(connection, :auto_recovery => true) do
        channel.on_error do |channel, close|
          Lolitra::logger.error("Channel error: #{channel}")
          Lolitra::logger.error(close)
        end
        block.call(channel)
      end
      channel 
    end

    def generate_queue_name(handler_class)
      if handler_class.respond_to?('queue_name')
        handler_class.queue_name
      else
        "#{@options[:queue_prefix]}#{MessageHandler::Helpers.underscore(handler_class.name)}#{@options[:queue_suffix]}"
      end
    end

    def generate_queue_name_dead(handler_class)
      "#{generate_queue_name(handler_class)}#{@options[:queue_dead_suffix]}"
    end

    def create_queue(message_class, handler_class, options)
      begin
        queue_name = generate_queue_name(handler_class)
        queue_name_dead = generate_queue_name_dead(handler_class)

        create_channel(self.connection) do |channel|
          begin
            channel.queue(queue_name, options.merge(@options[:queue_params])).bind(self.exchange, :routing_key => message_class.message_key)
            channel.queue(queue_name_dead, options.merge(@options[:queue_dead_params])).bind(self.exchange_dead_letter, :routing_key => message_class.message_key)
            channel.close
          rescue => e
            Lolitra::log_exception(e)
          end
        end
      
        if !@options[:no_consume] && !@channels[queue_name] #Only one subscriber by queue_name
          @channels[queue_name] = subscribe_to_messages(queue_name, options, handler_class)
        end
      rescue => e
          Lolitra::log_exception(e)
      end
    end

    def subscribe_to_messages(queue_name, options, handler_class)
      create_channel(self.connection) do |channel|
        channel.prefetch(1).queue(queue_name, options.merge(@options[:queue_params])).subscribe(:ack => true) do |info, payload|
          begin
            Lolitra::logger.debug("Message recived: #{info.routing_key}")
            Lolitra::logger.debug("#{payload}")
            message_class_tmp = handler_class.handlers[info.routing_key][0]
            handler_class.handle(message_class_tmp.unmarshall(payload))
            info.ack 
          rescue => e
            channel.reject(info.delivery_tag, false)
            Lolitra::log_exception(e)
            mark_deadletter
          end
        end
      end
    end

    def remove_mark_deadletter
      FileUtils.rm("#{Dir.pwd}/tmp/deadletter")
    end

    def mark_deadletter
      FileUtils.touch("#{Dir.pwd}/tmp/deadletter")
    end
  end
end
