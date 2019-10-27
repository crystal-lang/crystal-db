module DB
  annotation Field
  end

  # `DB::Field` properties:
  # * **ignore**: if `true`, skip this field in serialization and deserialization (`false` by default)
  # * **key**: defines which column to read from a `ResultSet` (name of the instance variable by default)
  # * **converter**: defines an alternate type for parsing results. The given class must define `from_rs(DB::ResultSet)` and return an instance of the included type.
  module Serializable
    macro included
      # Define a `new` directly in the type, like JSON::Serializable

      def self.new(rs : ::DB::ResultSet)
        instance = allocate
        instance.initialize(__set_for_db_serializable: rs)
        GC.add_finalizer(instance) if instance.responds_to?(:finalize)
        instance
      end

      # Inject the `new` into subclasses as well

      macro inherited
        def self.new(rs : ::DB::ResultSet)
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
                type: ivar.type,
                key: ((ann && ann[:key]) || ivar).id.stringify,
                default: ivar.default_value,
                nilable: ivar.type.nilable?,
                converter: ann && ann[:converter]
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
                %var{name} =
                  {% if value[:converter] %}
                    {{value[:converter]}}.from_rs(rs)
                  {% elsif value[:nilable] || value[:default] != nil %}
                    rs.read(::Union({{value[:type]}} | Nil))
                  {% else %}
                    rs.read({{value[:type]}})
                  {% end %}
            {% end %}
          else
            on_unknown_db_column(col_name)
          end
        end

        {% for key, value in properties %}
          {% unless value[:nilable] || value[:default] != nil %}
            if %var{key}.is_a?(Nil) && !%found{key}
              raise ::DB::MappingException.new("missing result set attribute: {{(value[:key] || key).id}}")
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
    end

    module Strict
      protected def on_unknown_db_column(col_name)
        raise ::DB::MappingException.new("unknown result set attribute: #{col_name}")
      end
    end
  end
end