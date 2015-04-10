require "spec_helper"
require "active_record"

describe ArFindInBatchesWithUsefulness do
  before :all do
    ActiveRecord::Base.establish_connection adapter: "postgresql"
    class Foo < ActiveRecord::Base
      belongs_to :baz
    end
    class Baz < ActiveRecord::Base
      has_many :foos
    end

    ActiveRecord::Schema.define do
      self.verbose = false

      create_table :foos, force: true do |t|
        t.integer :count
        t.integer :baz_id
      end
      create_table :bazs, force: true do |t|
        t.integer :count
      end
    end
  end

  before(:each) do
    Foo.delete_all
    Baz.delete_all
  end

  let! (:foos) { 5.times.map { Foo.create(count: rand(100)) } }

  describe "ordering" do

    it "maintains asc order in query" do
      results = []
      Foo.all.order(:count).find_in_batches(cursor: true) do |b|
        results << b
      end
      results.flatten!

      expect(results.map(&:count)).to eql results.map(&:count).sort
    end

    it "maintains desc order in query" do
      results = []
      Foo.all.order(count: :desc).find_in_batches(cursor: true) do |b|
        results << b
      end
      results.flatten!

      expect(results.map(&:count)).to eql results.map(&:count).sort.reverse
    end

    it "maintains order across batches" do
      results = []
      Foo.all.order(:count).find_in_batches(batch_size: 2, cursor: true) do |b|
        results << b
      end
      results.flatten!

      expect(results.map(&:count)).to eql results.map(&:count).sort
    end
  end

  describe "enum" do
    it "can chain enumerable methods" do
      expect(Foo.all.order(count: :desc).find_in_batches(batch_size: 2, cursor: true)).to respond_to(:collect_concat)
    end

    it "has the correct enum.size" do
      batch_size = 2
      expect(Foo.all.order(count: :desc).find_in_batches(batch_size: batch_size, cursor: true).size).to eql (foos.count / batch_size.to_f).ceil
    end
  end

  describe "start" do

    it "skips first x rows if start option given" do
      results = []
      Foo.all.find_in_batches(cursor: true, start: 2) do |b|
        results << b
      end
      results.flatten!
      expect(results.count).to eql (foos.count - 2)
    end

  end
end
