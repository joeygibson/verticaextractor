And(/^I have waited no more than (\d+) seconds for Vertica to be ready$/) do |max_wait_in_seconds|
  EventuallyHelper::eventually(max_wait_in_seconds.to_i) {
    rc = vertica_query("select now();")
    expect(rc.split(/\n/)[2].strip).to match(/^\d{4}-\d{2}-\d{2}/)
  }
end

And(/^there are no tables$/) do
    rc = vertica_query("select count(*) from tables")
    expect(rc.to_i).to eq(0)
end

def vertica_query(query)
    %x{vsql -h vertica -U dbadmin -c "#{query}" 2>&1}
end
