require 'csv'
require 'sqlite3'
require 'optparse'
require 'nokogiri'

max = nil

OptionParser.new do |opts|
  opts.banner = "Usage: load_db.rb [options]"
  opts.on("-m", "--max COUNT", Integer, "Only process COUNT documents") do |m|
    max = m
  end
end.parse!

$tags = Hash.new { |h,k| h[k] = 0 }
class TagCounter < Nokogiri::XML::SAX::Document
  
    def start_element(name, attrs)
      res = ($tags[name] += 1)
      if res == 1
        $stderr.puts("\n" + name + "\n")
      end
    end
end

counter = TagCounter.new
parser = Nokogiri::HTML::SAX::Parser.new(counter)

gets #skip first line
num = 0
CSV($<) do |csv_in|
  csv_in.each do |row|
    doc_id = row[0]
    title = row[1]
    body = row[2]
    if row.length >= 4
      tags = row[3]
    else
      tags = ""
    end
   
    parser.parse(body)

    if max
      max -= 1
      break if max == 0
    end
  end
end

$tags.keys.sort.each do |k|
  puts [k, $tags[k]].join("\t")
end
