module DB
  annotation Field
  end

  # The `DB::Serialization` module automatically generates methods for DB serialization when included.
  #
  # Once included, `ResultSet#read(t)` populates properties of the class from the
  # `ResultSet`.
  #
  # ### Example
  #
  # ```
  # require "db"
  #
  # class Employee
  #   include DB::Serializable
  #
  #   property title : String
  #   property name : String
  # end
  #
  # employees = Employee.from_rs(db.query("SELECT title, name FROM employees"))
  # employees[0].title # => "Manager"
  # employees[0].name  # => "John"
  # ```
  #
  # ### Usage
  #
  # `DB::Serializable` was designed in analogue with `JSON::Serializable`, so usage is identical.
  #  However, like `DB.mapping`, `DB::Serializable` is **strict by default**, so extra columns will raise `DB::MappingException`s.
  #
  # Similar to `JSON::Field`, there is an annotation `DB::Field` that can be used to set serialization behavior
  # on individual instance variables.
  #
  # ```
  # class Employee
  #   include DB::Serializable
  #
  #   property title : String
  #
  #   @[DB::Field(key: "firstname")]
  #   property name : String?
  # end
  # ```
  #
  # `DB::Field` properties:
  # * **ignore**: if `true`, skip this field in serialization and deserialization (`false` by default)
  # * **key**: defines which column to read from a `ResultSet` (name of the instance variable by default)
  # * **converter**: defines an alternate type for parsing results. The given type must define `#from_rs(DB::ResultSet)` and return an instance of the included type.
  #
  # ### `DB::Serializable::NonStrict`
  #
  # Including this module is functionally identical to passing `{strict: false}` to `DB.mapping`: extra columns will not raise.
  #
  # ```
  # class Employee
  #   include DB::Serializable
  #   include DB::Serializable::NonStrict
  #
  #   property title : String
  #   property name : String
  # end
  #
  # # does not raise!
  # employees = Employee.from_rs(db.query("SELECT title, name, age FROM employees"))
  # ```
  module Serializable
    macro included
      include ::DB::Mappable

      # Define a `new` and `from_rs` directly in the type, like JSON::Serializable
      # For proper overload resolution

      def self.new(rs : ::DB::ResultSet)
        instance = allocate
        instance.initialize(__set_for_db_serializable: rs)
        GC.add_finalizer(instance) if instance.responds_to?(:finalize)
        instance
      end

      def self.from_rs(rs : ::DB::ResultSet)
        objs = Array(self).new
        rs.each do
          objs << self.new(rs)
        end
        objs
      ensure
        rs.close
      end

      # Inject the class methods into subclasses as well

      macro inherited
        def self.new(rs : ::DB::ResultSet)
          super
        end

        def self.from_rs(rs : ::DB::ResultSet)
          super
        end
      end
    end

    def initialize(*, __set_for_db_serializable rs : ::DB::ResultSet)
      {% begin %}
        {% properties = {} of Nil => Nil %}
        {% for ivar in @type.instance_vars %}
          {% ann = ivar.annotation(::DB::Field) %}
          {% unless ann && ann[:ignore] %}
            {%
              properties[ivar.id] = {
                type:      ivar.type,
                key:       ((ann && ann[:key]) || ivar).id.stringify,
                default:   ivar.default_value,
                nilable:   ivar.type.nilable?,
                converter: ann && ann[:converter],
              }
            %}
          {% end %}
        {% end %}

        {% for name, value in properties %}
          %var{name} = nil
          %found{name} = false
        {% end %}

        rs.each_column do |col_name|
          case col_name
            {% for name, value in properties %}
              when {{value[:key]}}
                %found{name} = true
                begin
                  %var{name} =
                    {% if value[:converter] %}
                      {{value[:converter]}}.from_rs(rs)
                    {% elsif value[:nilable] || value[:default] != nil %}
                      rs.read(::Union({{value[:type]}} | Nil))
                    {% else %}
                      rs.read({{value[:type]}})
                    {% end %}
                rescue exc
                  ::raise ::DB::MappingException.new(exc.message, self.class.to_s, {{name.stringify}}, cause: exc)
                end
            {% end %}
          else
            rs.read # Advance set, but discard result
            on_unknown_db_column(col_name)
          end
        end

        {% for key, value in properties %}
          {% unless value[:nilable] || value[:default] != nil %}
            if %var{key}.nil? && !%found{key}
              ::raise ::DB::MappingException.new("Missing column {{value[:key].id}}", self.class.to_s, {{key.stringify}})
            end
          {% end %}
        {% end %}

        {% for key, value in properties %}
          {% if value[:nilable] %}
            {% if value[:default] != nil %}
              @{{key}} = %found{key} ? %var{key} : {{value[:default]}}
            {% else %}
              @{{key}} = %var{key}
            {% end %}
          {% elsif value[:default] != nil %}
            @{{key}} = %var{key}.is_a?(Nil) ? {{value[:default]}} : %var{key}
          {% else %}
            @{{key}} = %var{key}.as({{value[:type]}})
          {% end %}
        {% end %}
      {% end %}
    end

    protected def on_unknown_db_column(col_name)
      ::raise ::DB::MappingException.new("Unknown column: #{col_name}", self.class.to_s)
    end

    module NonStrict
      protected def on_unknown_db_column(col_name)
      end
    end
  end
end
