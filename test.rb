require './rrm'

conn = RRM.connect
conn.flushall

resource = RRM.register(conn, 'example', {:foo => {:type => :string},
                                          :bar => {:type => :list},
                                          :other => {:type => :hash}})

first = resource.create({:foo => 'foo', :bar => [1, 2], :other => {:a => :b}})
resource.create({:foo => 'second', :bar => [3, 4], :other => {:c => :d}})

first_found = resource.find(first.id)

puts "first_found"
p(first_found)

puts "\nfirst_found.foo"
p(first_found.foo)

puts "\nfirst_found.bar"
p(first_found.bar)

puts "\nfirst_found.other"
p(first_found.other)

puts "\nresource.all"
p(resource.all)
