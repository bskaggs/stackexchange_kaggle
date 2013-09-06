require 'csv'
require 'sqlite3'
require 'optparse'
require 'nokogiri'

clean = false
db = nil
max = nil

OptionParser.new do |opts|
  opts.banner = "Usage: load_db.rb [options]"

  opts.on("-c", "--clean", "Clean the HTML") do |c|
    clean = c
  end
  opts.on("-d", "--database DATABASE", "DATABASE file to load") do |dbfile|
    db = SQLite3::Database.new(dbfile)
  end
  opts.on("-m", "--max COUNT", Integer, "Only process COUNT documents") do |m|
    max = m
  end
end.parse!

db.execute ("CREATE TABLE questions (id integer, title text, body text, code text, links text, tags text); ")
stmt = db.prepare("INSERT INTO questions VALUES (?, ?, ?, ?, ?, ?);")

gets #skip first line
num = 0
db.transaction do |d|
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
      
      code = []
      links = []
      if clean
        doc = Nokogiri::HTML(body)
        doc.css('code').each { |node| code << node.text; node.remove }
        doc.css('a').each { |node| links << node.text }
        body = doc.text
      end
      stmt.execute(doc_id, title, body, code.join("\n"), links.join("\n"), tags)
      if max
        max -= 1
        break if max == 0
      end
    end
  end
end
stmt.close
db.close
