# Lolitra

TODO: Write a gem description

## Installation

Add this line to your application's Gemfile:

    gem 'lolitra'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install lolitra

## Usage

Create an event bus on initializers

    Lolitra::AmqpBus.new(
      :exchange => "exchangetest",
      :queue_prefix => "my_prefix_",
      :host => "127.0.0.1",
      :port => 5672,
      :user => "guest",
      :pass => "guest",
      :pull_subscribers => [DevicesHandler, FoldersHandler, UsersHandler]
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

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
