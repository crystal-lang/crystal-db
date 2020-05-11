require "./spec_helper"
require "base64"
require "json"

class SimpleModel
  include DB::Serializable

  property c0 : Int32
  property c1 : String
end

class NonStrictModel
  include DB::Serializable
  include DB::Serializable::NonStrict

  property c1 : Int32
  property c2 : String
end

class ModelWithDefaults
  include DB::Serializable

  property c0 : Int32 = 10
  property c1 : String = "c"
end

class ModelWithNilables
  include DB::Serializable

  property c0 : Int32? = 10
  property c1 : String?
end

class ModelWithNilUnionTypes
  include DB::Serializable

  property c0 : Int32 | Nil = 10
  property c1 : String | Nil
end

class ModelWithKeys
  include DB::Serializable

  @[DB::Field(key: "c0")]
  property foo : Int32
  @[DB::Field(key: "c1")]
  property bar : String
end

class ModelWithConverter
  module Base64Converter
    def self.from_rs(rs)
      Base64.decode(rs.read(String))
    end
  end

  include DB::Serializable

  @[DB::Field(converter: ModelWithConverter::Base64Converter)]
  property c0 : Slice(UInt8)
  property c1 : String
end

class ModelWithInitialize
  include DB::Serializable

  property c0 : Int32
  property c1 : String

  def_equals c0, c1

  def initialize(@c0, @c1)
  end
end

class ModelWithJSON
  include JSON::Serializable
  include DB::Serializable

  property c0 : Int32
  property c1 : String
end

macro from_dummy(query, type)
  with_dummy do |db|
    rs = db.query({{ query }})
    rs.move_next
    %obj = {{ type }}.new(rs)
    rs.close
    %obj
  end
end

macro expect_model(query, t, values)
  %obj = from_dummy({{ query }}, {{ t }})
  %obj.should be_a({{ t }})
  {% for key, value in values %}
    %obj.{{key.id}}.should eq({{value}})
  {% end %}
end

describe "DB::Serializable" do
  it "should initialize a simple model" do
    expect_model("1,a", SimpleModel, {c0: 1, c1: "a"})
  end

  it "should fail to initialize a simple model if types do not match" do
    expect_raises DB::MappingException, "Invalid Int32: b\n  deserializing SimpleModel#c0" do
      from_dummy("b,a", SimpleModel)
    end
  end

  it "should fail to initialize a simple model if there is a missing column" do
    expect_raises DB::MappingException, "Missing column c1\n  deserializing SimpleModel#c1" do
      from_dummy("1", SimpleModel)
    end
  end

  it "should fail to initialize a simple model if there is an unexpected column" do
    expect_raises DB::MappingException, "Unknown column: c2\n  deserializing SimpleModel" do
      from_dummy("1,a,b", SimpleModel)
    end
  end

  it "should initialize a non-strict model if there is an unexpected column" do
    expect_model("1,2,a,b", NonStrictModel, {c1: 2, c2: "a"})
  end

  it "should initialize a model with default values" do
    expect_model("1,a", ModelWithDefaults, {c0: 1, c1: "a"})
  end

  it "should initialize a model using default values if columns are missing" do
    expect_model("1", ModelWithDefaults, {c0: 1, c1: "c"})
  end

  it "should initialize a model using default values if values are nil and types are non nilable" do
    expect_model("1,NULL", ModelWithDefaults, {c0: 1, c1: "c"})
  end

  it "should initialize a model with nilables if columns are missing" do
    expect_model("1", ModelWithNilables, {c0: 1, c1: nil})
  end

  it "should initialize a model with nilables ignoring default value if NULL" do
    expect_model("NULL,a", ModelWithNilables, {c0: nil, c1: "a"})
  end

  it "should initialize a model with nil union types if columns are missing" do
    expect_model("1", ModelWithNilUnionTypes, {c0: 1, c1: nil})
  end

  it "should initialize a model with nil union types ignoring default value if NULL" do
    expect_model("NULL,a", ModelWithNilUnionTypes, {c0: nil, c1: "a"})
  end

  it "should initialize a model with different keys" do
    expect_model("1,a", ModelWithKeys, {foo: 1, bar: "a"})
  end

  it "should initialize a model with a value converter" do
    expect_model("Zm9v,a", ModelWithConverter, {c0: "foo".to_slice, c1: "a"})
  end

  it "should initialize a model with an initialize" do
    obj1 = from_dummy("1,a", ModelWithInitialize)
    obj2 = ModelWithInitialize.new(1, "a")
    obj1.should eq obj2
  end

  it "should initialize a model with JSON serialization also defined" do
    expect_model("1,a", ModelWithJSON, {c0: 1, c1: "a"})
  end

  it "should initialize multiple instances from a single resultset" do
    with_dummy do |db|
      db.query("1,a 2,b") do |rs|
        objs = SimpleModel.from_rs(rs)
        objs.size.should eq(2)
        objs[0].c0.should eq(1)
        objs[0].c1.should eq("a")
        objs[1].c0.should eq(2)
        objs[1].c1.should eq("b")
      end
    end
  end

  it "Class.from_rs should close resultset" do
    with_dummy do |db|
      rs = db.query("1,a 2,b")
      objs = SimpleModel.from_rs(rs)
      rs.closed?.should be_true

      objs.size.should eq(2)
      objs[0].c0.should eq(1)
      objs[0].c1.should eq("a")
      objs[1].c0.should eq(2)
      objs[1].c1.should eq("b")
    end
  end

  it "should initialize from a query_one" do
    with_dummy do |db|
      obj = db.query_one "1,a", as: SimpleModel
      obj.c0.should eq(1)
      obj.c1.should eq("a")
    end
  end

  it "should initialize from a query_all" do
    with_dummy do |db|
      objs = db.query_all "1,a 2,b", as: SimpleModel
      objs.size.should eq(2)
      objs[0].c0.should eq(1)
      objs[0].c1.should eq("a")
      objs[1].c0.should eq(2)
      objs[1].c1.should eq("b")
    end
  end
end
