require 'singleton'
require 'log4r'
require 'amqp'
require 'amqp/utilities/event_loop_helper'
require 'json'
require 'fiber'

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
        #puts "#{self.name} try to handle new message #{message.class.name}"
        begin      
          get_handler(message).handle(message)
        rescue NoMethodError => e
          raise NoHandlerMessageException.new(self, message) if e.message == "undefined method `handle' for nil:NilClass"
          raise
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

    def self.publish(message_instance)
      instance.publish(message_instance)
    end

    def publish(message_instance)
      bus.publish(message_instance)
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
      hash.each { |key, value| 
        method = "#{MessageHandler::Helpers.underscore(key)}="
        self.send(method, value) if self.respond_to?(method)
      }
    end

    def to_hash
      hash = {}
      self.instance_variables.each {|var| hash[var.to_s.delete("@").to_sym] = self.instance_variable_get(var) }
      hash
    end

    def marshall
      JSON.generate(to_hash)
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
      params = hash.reject { |key, value| !value }
      raise "no :exchange specified" unless hash[:exchange]

      self.queue_prefix = hash[:queue_prefix]||""

      AMQP::Utilities::EventLoopHelper.run do
        self.connection = AMQP.connect(params)
        self.exchange = AMQP::Channel.new(self.connection).topic(params[:exchange], :durable => true)
      end
    end

    def subscribe(message_class, handler_class)
      create_queue(message_class, handler_class, {:exclusive => true, :durable => false}, "")
    end

    def pull_subscribe(message_class, handler_class)
      create_queue(message_class, handler_class, {:durable => true}, queue_prefix + MessageHandler::Helpers.underscore(handler_class.name))
    end

    def publish(message)
      Lolitra::logger.debug("Message sent: #{message.class.message_key}")
      Lolitra::logger.debug("#{message.marshall}")
      self.exchange.publish(message.marshall, :routing_key => message.class.message_key, :timestamp => Time.now.to_i)
    end

  private
    def create_queue(message_class, handler_class, options, queue_name)
      EM.next_tick do
        channel = AMQP::Channel.new(self.connection)
        channel.prefetch(1).queue(queue_name, options).bind(self.exchange, :routing_key => message_class.message_key).subscribe(:ack => true) do |info, payload|
          Fiber.new do
            current_fiber = Fiber.current
            for i in (0..5)
              begin
                Lolitra::logger.debug("Message recived: #{info.routing_key}")
                Lolitra::logger.debug("#{payload}")
                message_class_tmp = handler_class.handlers[info.routing_key][0]
                handler_class.handle(message_class_tmp.unmarshall(payload))
                info.ack
                Lolitra::logger.debug("Message processed")
                break
              rescue => e
                Lolitra::logger.error("Try #{i}: #{e.message}")
                if (i!=5)
                  EventMachine.add_timer(5) do
                    current_fiber.resume
                  end
                  Fiber.yield 
                else
                  Lolitra::logger.error(e.backtrace.join("\n\t"))
                  info.reject(:requeue => false)
                end
              end
            end
          end.resume
        end
      end
    end
  end
end  
