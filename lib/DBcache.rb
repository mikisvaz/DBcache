require 'mysql'
module DBcache
  @@info = nil

  def self.config(info)
    @@info=info
  end

  def self.info
    @@info
  end

  def self.driver
    @@driver ||= Mysql::new(info[:dbhost], info[:dbuser], info[:dbpass], info[:dbname] )
    @@driver
  end

  def self.has_table?(table)
    driver.query("SHOW TABLES").each{|row|
      return true if row.include? table
    }
    false
  end

  def self.has_id?(table, id)
    driver.query("SELECT id FROM #{ table } WHERE id = #{ process(id) }").num_rows == 1
  end

  def self.delete(table, id)
    driver.query("DELETE FROM #{ table } WHERE id = #{ process(id) }")
  end

  def self.fast_add(table, id, values)
    driver.query("INSERT INTO #{ table }  VALUE(#{process(id)}, #{values.collect{|v| process(v)}.join(", ")})")
  end

  def self.add(table, id, values)
    values = values.collect{|v| process(v)}
    create(table, field_type(id), values.collect{|v| field_type(v)}) unless has_table?(table)
    delete(table, id) if has_id?(table, id) 
    driver.query("INSERT INTO #{ table }  VALUE(#{process(id)}, #{values.join(", ")})")
  end

  def self.field_type(value)
    case
    when Symbol === value
      "CHAR(50)"
    when String === value
      "VARCHAR(255)"
    when Integer === value
      "INT"
    end
  end

  def self.num_rows(table, field = '*')
    driver.query("SELECT COUNT(#{ field }) FROM #{ table }").fetch_row.first.to_i
  end

  def self.matches(table, ids)
    return [] if ids.empty?
    matches = []
    driver.query("SELECT id FROM #{ table } WHERE id IN (#{ ids.collect{|id| process(id) }.join(", ")}) ").each{|row| matches << row.first}
    matches
  end

  def self.process(value)
    case
    when value.nil?
      "NULL"
    when Symbol === value  
      return "'" + Mysql.escape_string(value.to_s) + "'"
    when String === value  
      if value.length == 0 
        "NULL"
      elsif value.length > 256
        return "'" + Mysql.escape_string(value.scan(/^.{253}/).first) + '...' + "'"
      else
        return "'" + Mysql.escape_string(value) + "'"
      end
    else
      value
    end
  end


  def self.drop(table)
    db = driver
    begin
      db.query("DROP TABLE #{ table }")
    rescue
    end
  end

  def self.create(table, id_type, value_types)
    db = driver
    db.query("CREATE TABLE #{ table } ( id #{ id_type }, #{
      i = -1
      value_types.collect{|type|
        i += 1
        "C#{i} #{type}"
      }.join(", ")
    }, PRIMARY KEY(id) )" )

  end

  def self.save(table, info, value_types = nil)
    drop(table)

    if Array === info
      hash = {}
      info.each_with_index{|v,i| hash[i] = v}
      info = hash
    end

    if value_types.nil?
      template = info.values.select{|list| 
        if Array === list
          list.select{|v| v.nil?}.empty?
        else
          list != nil
        end
      }.first

      template = [template] unless Array === template
      value_types =  template.collect{|f| field_type(f)}
    end

    create(table, field_type(info.keys.first), value_types)
    
    db = driver
    info.keys.each{|k|
      values = info[k].collect{|v| process(v)}
      db.query("INSERT INTO #{ table }  VALUE(#{process(k)}, #{values.join(", ")})")
    }
  end


  def self.load(table, ids = nil)
    db = driver
    data = {}

    if ids.nil?
      db.query("SELECT * FROM #{ table }").each{|row|
        data[row.shift] = row
      }
    else
      ids = [ids] unless Array === ids
      return data if ids.empty?

      db.query("SELECT * FROM #{ table } WHERE id IN (#{ids.collect{|v| process(v)}.join(", ")})").each{|row|
        data[row.shift] = row
      }
    end

    data
  end
end

