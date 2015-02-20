require 'singleton'
require 'log4r'
require 'amqp'
require 'amqp/utilities/event_loop_helper'
require 'json'

module Lolitra
  include Log4r

  @@logger = Logger.new 'lolitra'
  @@logger.outputters = Outputter.stdout

  def self.logger
    @@logger
  end

  def self.logger=(new_logger)
    @@logger = new_logger
  end

  def self.log_exception(e)
    logger.error(e.message)
    logger.error(e.backtrace.join("\n\t"))
  end

  def self.publish(message)
    Lolitra::MessageHandlerManager.publish(message)
  end

  module MessageHandler
    module Helpers
      def self.underscore(arg)
        word = arg.dup
        word.gsub!(/::/, '/')
        word.gsub!(/([A-Z]+)([A-Z][a-z])/,'\1_\2')
        word.gsub!(/([a-z\d])([A-Z])/,'\1_\2')
        word.tr!("-", "_")
        word.downcase!
        word
      end
    end

    class NoHandlerMessageException < StandardError
      def initialize(handler, message)
        @handler_class = handler.name
        @message_class = message.class.name
      end

      def to_s
        "No handler (or starter if stateful) for message #{@message_class} in class #{@handler_class}"
      end
    end

    module MessageHandlerClass
      
      def self.extended(base)
        class << base
          attr_accessor :handlers
          attr_accessor :starters
          attr_accessor :is_stateful
        end
        base.handlers = {}
        base.starters = []
        base.is_stateful = false
      end

      def handle(message)
        begin      
          get_handler(message).handle(message)
        rescue => e
          Lolitra::log_exception(e)
        end
      end
      
      def publish(message)
        #TODO: IoC
        MessageHandlerManager.publish(message)
      end

      def handle_messages
        handlers.values.collect { |class_method_pair| class_method_pair[0] }
      end

    private
      def message_handler(message_class, id = :id)   
        message_class.new.send(id) #check if id exists for this class
        handlers.merge!(message_class.message_key => [message_class, get_method_by_class(message_class), id])
      end
     
      def started_by(message_class, id = :id)
        starters << message_class.message_key
        message_handler(message_class, id)
      end

      def search(message)
        id_method_name = handlers[message.class.message_key][2]
        send("find_by_#{id_method_name}", message.send(id_method_name))
      end

      def is_starter?(message)
        starters.include? message.class.message_key
      end

      def get_handler(message)
        if is_stateful?
          is_starter?(message) ? (search(message) || new) : search(message)
        else
          new
        end
      end

      def get_method_by_class(arg_class)
        MessageHandler::Helpers.underscore(arg_class.name).gsub("/","_").to_sym
      end

      def stateful(stateful_arg)
        self.is_stateful = stateful_arg
      end

      def is_stateful?
        is_stateful
      end
    end

    def self.included(base)
      base.send :extend, MessageHandlerClass
    end

    def publish(message)
      self.class.publish(message)
    end

    def handle(message)
      handler_method = self.class.handlers[message.class.message_key][1]
      raise "Can't handle message #{message.class}" unless handler_method
      self.send(handler_method, message)
    end

  end

  class MessageHandlerManager
    include Singleton

    attr_accessor :bus

    def self.bus=(new_bus)
      instance.bus = new_bus
    end

    def self.bus
      instance.bus
    end

    def self.register_subscriber(handler_class)
      instance.register_subscriber(handler_class)
    end
   
    def register_subscriber(handler_class)
      handler_class.handle_messages.each do |message_class|
        bus.subscribe(message_class, handler_class)
      end
    end

    def self.register_pull_subscriber(handler_class)
      instance.register_pull_subscriber(handler_class)
    end
   
    def register_pull_subscriber(handler_class)
      handler_class.handle_messages.each do |message_class|
        bus.pull_subscribe(message_class, handler_class)
      end
    end

    def self.publish(message)
      instance.publish(message)
    end

    def publish(message)
      Lolitra::logger.debug("Message sent: #{message.class.message_key}")
      Lolitra::logger.debug("#{message.marshall}")
      bus.publish(message)
    end
  end

  module Message

    module MessageClass
      def self.extended(base)
        class << base; attr_accessor :class_message_key; end
      end

      def message_key(key = nil)
        if (key)
          self.class_message_key = key      
        else
          self.class_message_key || "#{MessageHandler::Helpers.underscore(self.class.name)}"
        end
      end

      def unmarshall(message_json)
        hash = JSON.parse(message_json)
        self.new(hash)
      end

    end

    def self.included(base)
      base.send :extend, MessageClass
    end

    def initialize(hash={})
      super()
      hash.keys.each do |key|
        self.send "#{key}=", hash[key] if self.respond_to? "#{key}="
      end
    end

    def marshall
      JSON.generate(self)
    end
  end

  class FayeBus
    def initialize(options = {})
      EM::next_tick do
        @socketClient = Faye::Client.new(options[:url] || 'http://localhost:9292/faye')
      end
    end
    
    def subscribe(message_class, handler_class)
      EM::next_tick do
        @socketClient.subscribe(message_class.message_key) do |payload|
          Lolitra::logger.debug("Message recived:")
          Lolitra::logger.debug("#{payload}")
          handler_class.handle(message_class.unmarshall(payload))
        end
      end
    end

    def publish(message)
      @socketClient.publish(message.class.message_key, message.marshall)
    end 
  end

  class AmqpBus
    attr_accessor :queue_prefix
    attr_accessor :connection
    attr_accessor :exchange

    def initialize(hash = {})
      Lolitra::MessageHandlerManager.bus = self

      @channels = {}
      @params = hash.reject { |key, value| !value }
      raise "no :exchange specified" unless hash[:exchange]

      self.queue_prefix = hash[:queue_prefix]||""
      AMQP::Utilities::EventLoopHelper.run do
        self.connection = AMQP.start(@params) do |connection|
          channel = create_channel(connection) do |channel|
            begin
              self.exchange = channel.topic(@params[:exchange], :durable => true)

              @params[:pull_subscribers].each do |handler|
                Lolitra::MessageHandlerManager.register_pull_subscriber(handler)
              end
            rescue => e
              Lolitra::logger.debug("error")
              Lolitra::log_exception(e)
            end
          end
        end
      end
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
      channel = AMQP::Channel.new(connection) do
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
          channel.queue(queue_name, options).bind(self.exchange, :routing_key => message_class.message_key)
          channel.close
        end
      
        if !@channels[queue_name] #Only one subscriber by queue_name
          @channels[queue_name] = create_channel(self.connection) do |channel|
            channel.prefetch(1).queue(queue_name, options).subscribe do |info, payload|
              begin
                Lolitra::logger.debug("Message recived: #{info.routing_key}")
                Lolitra::logger.debug("#{payload}")
                message_class_tmp = handler_class.handlers[info.routing_key][0]
                handler_class.handle(message_class_tmp.unmarshall(payload))
              rescue => e
                Lolitra::log_exception(e)
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
