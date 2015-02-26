# Lolitra

Amqp and Faye event bus

## Installation

Add this line to your Gemfile:

    gem 'lolitra'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install lolitra

## Usage

Create an event bus on initializers

    Lolitra::RabbitmqBus.new(
      :exchange => "exchangetest",
      :host => "127.0.0.1",
      :port => 5672,
      :user => "guest",
      :pass => "guest",
      :pull_subscribers => [DevicesHandler]
    )

Create messages

    class DeviceCreated
      include Lolitra::Message

      attr_accessor :id, :name

      message_key 'assets.device.created'
    end

    class DeviceMoved
      include Lolitra::Message

      attr_accessor :id

      message_key 'assets.device.moved'
    end

Create a message handler

    class DevicesHandler
      include Lolitra::MessageHandler 

      message_handler DeviceCreated, :id
      message_handler DeviceMoved, :id

      stateful false

      def device_created(message)
        Device.new({:id => message.id})
      end
       
      def deivce_moved(message)
        Device.move({:id => message.id})
      end
       
    end

*Rabbitmq*
Lolitra generates a deadletter exchange and queues to handle dead letters and will be aware about connections issues, reconnecting on every failure.

*Deadletter manual handling*
With lolitra you can recover dead letter message with irb or rails console.

```
Lolitra::subscribers 
```
will return the available handlers

```
Lolitra::process_deadletter_messages(DevicesHandler)
```
will process all dead letter from DevicesHandler until found an exception
Fail recover deadletter will reenqueue to dead letter queue

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
