require 'singleton'
require 'log4r'
require 'amqp'
require 'amqp/utilities/event_loop_helper'
require 'json'
require 'fileutils'
require_relative 'rabbitmq_bus'

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

  def self.unsubscribe(handler_class, &block)
    Lolitra::MessageHandlerManager.unsubscribe(handler_class, &block)
  end

  def self.disconnect(&block)
    Lolitra::MessageHandlerManager.disconnect(&block)
  end

  def self.subscribers
    Lolitra::MessageHandlerManager.instance.subscribers.collect do |subscriber|
      subscriber.name
    end
  end

  def self.process_deadletters(subscriber)
    Lolitra::MessageHandlerManager.instance.process_deadletters(subscriber)
  end

  def self.remove_next_deadletter(subscriber)
    Lolitra::MessageHandlerManager.instance.remove_next_deadletter(subscriber)
  end

  def self.purge_deadletters(subscriber)
    Lolitra::MessageHandlerManager.instance.purge_deadletters(subscriber)
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
        get_handler(message).handle(message)
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
    attr_accessor :subscribers

    def self.bus=(new_bus)
      instance.bus = new_bus
    end

    def self.bus
      instance.bus
    end

    def self.register_subscriber(handler_class)
      instance.register_subscriber(handler_class)
    end
   
    def subscribers
      @subscribers ||= []
    end

    def process_deadletters(handler_class)
      bus.process_deadletters(handler_class)
    end

    def purge_deadletters(handler_class)
      bus.purge_deadletters(handler_class)
    end

    def remove_next_deadletter(handler_class)
      bus.remove_next_deadletter(handler_class)
    end

    def register_subscriber(handler_class)
      subscribers << handler_class
      handler_class.handle_messages.each do |message_class|
        bus.subscribe(message_class, handler_class)
      end
    end

    def self.unsubscribe(handler_class, &block)
      instance.unsubscribe(handler_class, &block)
    end

    def unsubscribe(handler_class, &block)
      Lolitra::logger.info("Unsubscribing #{handler_class}")
      bus.unsubscribe(handler_class, &block)
    end

    def self.publish(message)
      instance.publish(message)
    end

    def publish(message)
      Lolitra::logger.debug("Message sent: #{message.class.message_key}")
      Lolitra::logger.debug("#{message.marshall}")
      bus.publish(message)
    end

    def self.disconnect(&block)
      instance.disconnect(&block)
    end

    def disconnect(&block)
      bus.disconnect(&block)
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
end  
