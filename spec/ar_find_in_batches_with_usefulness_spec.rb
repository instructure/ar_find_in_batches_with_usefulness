require "spec_helper"
require "active_record"

describe ArFindInBatchesWithUsefulness do
  before :all do
    ActiveRecord::Base.establish_connection adapter: "postgresql"
  end

  before(:each) do
    class Foo < ActiveRecord::Base; end

    ActiveRecord::Schema.define do
      self.verbose = false

      create_table :foos, force: true do |t|
        t.integer :count
      end
    end
  end

  after(:each) do
    Object.send(:remove_const, :Foo)
  end

  describe "ordering" do
    let! (:items) { 5.times.map { Foo.create(count: rand(100)) } }

    it "maintains asc order in query" do
      results = []
      Foo.all.order(:count).find_in_batches_with_cursor do |b|
        results << b
      end
      results.flatten!

      expect(results.map(&:count)).to eql results.map(&:count).sort
    end

    it "maintains desc order in query" do
      results = []
      Foo.all.order(count: :desc).find_in_batches_with_cursor do |b|
        results << b
      end
      results.flatten!

      expect(results.map(&:count)).to eql results.map(&:count).sort.reverse
    end

    it "maintains order across batches" do
      results = []
      Foo.all.order(:count).find_in_batches_with_cursor(batch_size: 2) do |b|
        results << b
      end
      results.flatten!

      expect(results.map(&:count)).to eql results.map(&:count).sort
    end

  end
end
