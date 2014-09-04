$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", '..', "lib")
require 'minitest/spec'
require 'orocos'
require 'orocos/test'
require 'orocos/async'

MiniTest::Unit.autorun

TEST_DIR = File.expand_path('..', File.dirname(__FILE__))
DATA_DIR = File.join(TEST_DIR, 'data')
WORK_DIR = File.join(TEST_DIR, 'working_copy')

# helper for generating an ior from a name
def ior(name)
    Orocos.name_service.ior(name)
rescue Orocos::NotFound => e
    "IOR:010000001f00000049444c3a5254542f636f7262612f435461736b436f6e746578743a312e300000010000000000000064000000010102000d00000031302e3235302e332e31363000002bc80e000000fe8a95a65000004d25000000000000000200000000000000080000000100000000545441010000001c00000001000000010001000100000001000105090101000100000009010100"
end

describe Orocos::Async::CORBA::OutputPort do
    include Orocos::Spec

    before do 
        Orocos::Async.clear
    end

    describe "When connect to a remote task" do 
        it "must return a reader for output ports" do 
            Orocos.run('simple_source') do
                t1 = Orocos::Async::CORBA::TaskContext.new(ior('simple_source_source'))
                port = t1.port("cycle")
                port.reader.must_be_kind_of Orocos::Async::CORBA::OutputReader
            end
        end
        it "must asynchronously return a reader for output ports" do 
            Orocos.run('simple_source') do
                t1 = Orocos::Async::CORBA::TaskContext.new(ior('simple_source_source'))
                port = t1.port("cycle")
                reader = nil
                port.reader do |r|
                    reader = r
                end
                sleep 0.1
                Orocos::Async.step
                reader.must_be_kind_of Orocos::Async::CORBA::OutputReader
            end
        end
        it "must call on_data if new data are available and a block is registered" do 
            Orocos.run('simple_source') do
                t1 = Orocos::Async::CORBA::TaskContext.new(ior('simple_source_source'))
                port = t1.port("cycle")
                data = []
                s = port.on_data :period => 0.05 do |d|
                    data << d
                end
                t1.configure
                t1.start
                1.upto(10) do
                    Orocos::Async.step
                    sleep 0.05
                end
                t1.stop
                assert !data.empty?
                data.each_with_index do |v,i|
                    assert_equal i+1,v
                end
            end
        end
    end
end

describe Orocos::Async::CORBA::InputPort do
    include Orocos::Spec

    describe "When connect to a remote task" do 
        it "must return a writer for input ports" do 
            Orocos.run('simple_sink') do
                t1 = Orocos::Async::CORBA::TaskContext.new(ior('simple_sink_sink'))
                port = t1.port("cycle")
                port.writer.must_be_kind_of Orocos::Async::CORBA::InputWriter
            end
        end

        it "must asynchronously return a writer for input ports" do 
            Orocos.run('simple_sink') do
                t1 = Orocos::Async::CORBA::TaskContext.new(ior('simple_sink_sink'))
                port = t1.port("cycle")
                writer = nil
                port.writer do |w|
                    writer = w
                end
                sleep 0.1
                Orocos::Async.step
                writer.must_be_kind_of Orocos::Async::CORBA::InputWriter
            end
        end
    end
end

describe Orocos::Async::CORBA::OutputReader do
    include Orocos::Spec

    describe "When connect to a remote task" do 
        it "should be possible to read data" do 
            Orocos.run('simple_source') do
                data = []
                t1 = Orocos::Async::CORBA::TaskContext.new(ior('simple_source_source'))
                port = t1.port("cycle")
                reader = port.reader(:type => :buffer,:size => 10)

                #for now operations are not possible
                t1.configure
                t1.start

                1.upto(5) do |v|
                    sleep 0.5
                    assert_equal v,reader.read
                end
            end
        end

        it "should call on data each time new data are available" do 
            Orocos.run('simple_source') do
                data = []
                t1 = Orocos::Async::CORBA::TaskContext.new(ior('simple_source_source'))
                port = t1.port("cycle")
                reader = port.reader(:period => 0.09)
                data = []
                reader.on_data do |val|
                    data << val
                end

                t1.configure
                t1.start
                1.upto(10) do 
                    Orocos::Async.step
                    sleep 0.05
                end
                t1.stop
                assert !data.empty?
                data.each_with_index do |v,i|
                    assert_equal i+1,v
                end
            end
        end

        it "should read all data from a buffered connection even if the period of the reader is too large" do 
            Orocos.run('simple_source') do
                data = []
                t1 = Orocos::Async::CORBA::TaskContext.new(ior('simple_source_source'))
                port = t1.port("cycle",:type => :buffer,:size => 100,:period => 100)
                port.on_data do |sample|
                    data << sample
                end
                t1.configure
                t1.start
                1.upto(5) do |v|
                    Orocos::Async.steps
                    sleep 0.5
                end
                assert !data.empty?
            end
        end
    end
end
