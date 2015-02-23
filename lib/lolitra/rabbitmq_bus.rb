module Lolitra
  class RabbitmqBus
    attr_accessor :queue_prefix
    attr_accessor :connection
    attr_accessor :exchange
    attr_accessor :exchange_dead_letter
    attr_accessor :options
    attr_accessor :options_dead

    def initialize(hash = {})
      Lolitra::MessageHandlerManager.bus = self

      @channels = {}
      @params = hash.reject { |key, value| !value }
      raise "no :exchange specified" unless hash[:exchange]

      self.queue_prefix = hash[:queue_prefix]||""
      AMQP::Utilities::EventLoopHelper.run do
        self.connection = AMQP.start(@params) do |connection|
          Lolitra::logger.info("Connected to rabbitmq.")
          channel = create_channel(connection) do |channel|
            begin
              self.exchange = channel.topic(@params[:exchange], :durable => true)
              self.exchange_dead_letter = channel.topic("#{@params[:exchange]}.dead", :durable => true)

              @params[:pull_subscribers].each do |handler|
                Lolitra::MessageHandlerManager.register_pull_subscriber(handler)
              end
            rescue => e
              Lolitra::logger.debug("error")
              Lolitra::log_exception(e)
            end
          end
        end
        self.connection.on_tcp_connection_loss do |connection, settings|
          # reconnect in 10 seconds, without enforcement
          Lolitra::logger.error("Connection loss. Trying to reconnect in 10 secs")
          connection.reconnect(false, 10)
        end
      end
    end

    def options
      @options || {
        :arguments => {"x-dead-letter-exchange" => exchange_dead_letter.name}
      }
    end

    def options_dead
      @options || {}
    end

    def subscribe(message_class, handler_class)
      create_queue(message_class, handler_class, {:exclusive => true, :durable => false}, "")
    end

    def pull_subscribe(message_class, handler_class)
      create_queue(message_class, handler_class, {:durable => true})
    end

    def publish(message)
      #TODO: if exchange channel is closed doesn't log anything
      self.exchange.publish(message.marshall, :routing_key => message.class.message_key, :timestamp => Time.now.to_i)
    end

  private
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

    def create_queue(message_class, handler_class, options)
      begin
        queue_name = queue_prefix + MessageHandler::Helpers.underscore(handler_class.name)

        create_channel(self.connection) do |channel|
          begin
            channel.queue(queue_name, options.merge(self.options)).bind(self.exchange, :routing_key => message_class.message_key)
            channel.queue("#{queue_name}.dead", options.merge(self.options_dead)).bind(self.exchange_dead_letter, :routing_key => message_class.message_key)
            channel.close
          rescue => e
            Lolitra::log_exception(e)
          end
        end
      
        if !@channels[queue_name] #Only one subscriber by queue_name
          @channels[queue_name] = create_channel(self.connection) do |channel|
            channel.prefetch(1).queue(queue_name, options).subscribe(:ack => true) do |info, payload|
              begin
                Lolitra::logger.debug("Message recived: #{info.routing_key}")
                Lolitra::logger.debug("#{payload}")
                message_class_tmp = handler_class.handlers[info.routing_key][0]
                handler_class.handle(message_class_tmp.unmarshall(payload))
                info.ack 
              rescue => e
                channel.reject(info.delivery_tag, false)
                Lolitra::log_exception(e)
                FileUtils.touch("#{Dir.pwd}/tmp/deadletter")
              end
            end
          end
        end
      rescue => e
          Lolitra::log_exception(e)
      end
    end
  end
end
