require 'csv'

tags = Hash.new {|h,k| h[k] = 0 }
CSV($stdin) do |csv_in|
  csv_in.each do |row|
    row[3].split(/\s/).each { |x| tags[x] += 1}
  end
end

tags.keys.sort_by {|x| tags[x] }.each { |x| puts [tags[x],x].join("\t")}
