require "./spec_helper"

describe DB::Connection do
  it "raises on #driver_name if the driver doesn't override it" do
    with_dummy_connection do |cnn|
      cnn.driver_name.should eq "dummy"
    end
  end

  it "returns nil for #server_name if the driver doesn't override it" do
    with_dummy_connection do |cnn|
      cnn.server_name.should be_nil
    end
  end

  it "returns nil for #server_version if the driver doesn't override it" do
    with_dummy_connection do |cnn|
      cnn.server_version.should be_nil
    end
  end
end
