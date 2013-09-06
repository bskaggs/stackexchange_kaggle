require 'csv'
require 'sqlite3'

db = SQLite3::Database.new("Test.db")
db.execute (" CREATE TABLE questions (id integer, title text, body text); ")
stmt = db.prepare("INSERT INTO questions VALUES (?, ?, ?);")
num = 0
db.transaction do |d|
  CSV($stdin) do |csv_in|
    csv_in.each do |row|
      num += 1
      next if num == 1
      stmt.execute(*row)
    end
  end
end
stmt.close
db.close
