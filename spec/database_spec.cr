require "./spec_helper"

describe DB::Database do
  it "allows connection initialization" do
    cnn_setup = 0
    DB.open "dummy://localhost:1027?initial_pool_size=2&max_pool_size=4&max_idle_pool_size=1" do |db|
      cnn_setup.should eq(0)

      db.setup_connection do |cnn|
        cnn_setup += 1
      end

      cnn_setup.should eq(2)

      db.using_connection do
        cnn_setup.should eq(2)
        db.using_connection do
          cnn_setup.should eq(2)
          db.using_connection do
            cnn_setup.should eq(3)
            db.using_connection do
              cnn_setup.should eq(4)
            end
            # the pool didn't shrink no new initialization should be done next
            db.using_connection do
              cnn_setup.should eq(4)
            end
          end
          # the pool shrink 1. max_idle_pool_size=1
          # after the previous end there where 2.
          db.using_connection do
            cnn_setup.should eq(4)
            # so now there will be a new connection created
            db.using_connection do
              cnn_setup.should eq(5)
            end
          end
        end
      end
    end
  end
end
