require 'redis'

module RRM
  @resource_map = {}

  def self.connect(options = {})
    Redis.new(options)
  end

  def self.register(conn, name, attributes = {})
    @resource_map[name] = Resource.new(conn, name, attributes)
  end

  class Resource
    attr_reader :id

    def initialize(conn, name, attribute_definitions)
      @conn = conn
      @name = name

      @attributes = attribute_definitions.map do |attr_name, definition|
        case definition[:type]
        when :string then StringAttr.new(conn, @name, attr_name)
        when :list   then ListAttr.new(conn, @name, attr_name)
        when :hash   then HashAttr.new(conn, @name, attr_name)
        else raise "Unknow attribute type for definition #{definition}"
        end
      end


      @struct = Struct.new(*attribute_definitions.keys.unshift(:id))
    end

    def create(values = {})
      instance = @struct.new(incr_id)

      @conn.pipelined do
        @conn.rpush index_key, instance[:id]
        @attributes.each do |attr|
          attr.create(instance[:id], values[attr.name])
          instance[attr.name] = values[attr.name]
        end
      end

      instance
    end

    def all
      ids       = @conn.lrange index_key, 0, -1
      instances = ids.map { |id| @struct.new(id) }

      @conn.pipelined do
        instances.each { |instance| load_attributes(instance, @attributes) }
      end

      instances.each { |instance| load_futures(instance, @attributes) }

      instances
    end

    def find(id)
      instance = @struct.new(id)

      @conn.pipelined { load_attributes(instance, @attributes) }
      load_futures(instance, @attributes)

      instance
    end

    def incr_id
      @conn.incr counter_key
    end

    private
    def counter_key
      "#{@name}::_counter"
    end

    def index_key
      "#{@name}::_index"
    end

    def load_attributes(instance, attributes)
      attributes.each do |attr|
        instance[attr.name] = attr.find(instance[:id])
      end
    end

    def load_futures(instance, attributes)
      attributes.each do |attr|
        instance[attr.name] = instance[attr.name].value
      end
    end
  end

  class Attribute
    attr_reader :name

    def initialize(conn, prefix, name)
      @conn   = conn
      @prefix = prefix
      @name   = name
    end

    def key(id)
      "#{@prefix}::#{id}::#{@name}"
    end
  end

  class StringAttr < Attribute
    def create(id, value)
      @conn.set key(id), value
    end

    def find(id)
      @conn.get key(id)
    end
  end

  class ListAttr < Attribute
    def create(id, value)
      @conn.rpush key(id), value
    end

    def find(id)
      @conn.lrange key(id), 0, -1
    end
  end

  class HashAttr < Attribute
    def create(id, value)
      @conn.hmset key(id), value.to_a.flatten
    end

    def find(id)
      @conn.hgetall key(id)
    end
  end
end
