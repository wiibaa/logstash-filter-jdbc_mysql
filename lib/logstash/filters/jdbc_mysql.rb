# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "logstash/filters/jdbc"

# This filter executes a SQL query and store the result set in the field 
# specified as `target`.
# 
# This filter is a wrapper around the generic jdbc filter for
# 1. taking care of loading the necessary jdbc driver through a gem depencendy
# 2. simplified configuration to mysql database.
# For example the following configuration:
#
# [source,ruby]
# filter {
#   jdbc_mysql {
#     host => "localhost"
#     default_schema = "mydatabase" 
#     user => "me"
#     password => "secret"
#     statement => "select * from WORLD.COUNTRY WHERE Code = :code"
#     parameters => { "code" => "country_code"}
#     target => "country_details"
#   }
# }
#
# is equivalent to the generic configuration
# [source,ruby]
# filter {
#   jdbc {
#     jdbc_driver_library => "/path/to/mysql-connector-java-5.1.34-bin.jar"
#     jdbc_driver_class => "com.mysql.jdbc.Driver"
#     jdbc_connection_string => "jdbc:mysql://localhost:3306/mydatabase"
#     jdbc_user => "me"
#     jdbc_password => "secret"
#     statement => "select * from WORLD.COUNTRY WHERE Code = :code"
#     parameters => { "code" => "country_code"}
#     target => "country_details"
#   }
# }
#
class LogStash::Filters::JdbcMysql < LogStash::Filters::Base

  config_name "jdbc_mysql"

  # Host of the mysql server to connect to with jdbc
  config :host, :validate => :string, :required => true 

  # Port  of the mysql server to connect to with jdbc
  config :port, :validate => :number, :default => 3306

  # Database username
  config :user, :validate => :string

  # Database user password
  config :password, :validate => :password

  # Database default schema
  config :default_schema, :validate => :string

  # Statement to execute.
  # To use parameters, use named parameter syntax, for example "SELECT * FROM MYTABLE WHERE ID = :id"
  config :statement, :validate => :string, :required => true 

  # Hash of query parameter, for example `{ "id" => "id_field" }`
  config :parameters, :validate => :hash, :default => {}

  # Target field to store the result set.
  # Field is overwritten if exists.
  config :target, :validate => :string, :required => true 

  # Append values to the `tags` field if sql error occured
  config :tag_on_failure, :validate => :array, :default => ["_jdbcfailure"] 


  public
  def register
    require "jdbc/mysql"
    Jdbc::MySQL.load_driver
    @jdbc_filter = LogStash::Filters::Jdbc.new(
     "jdbc_driver_class" => "com.mysql.jdbc.Driver",
     "jdbc_connection_string" => "jdbc:mysql://#{host}:#{port}/#{default_schema}",
     "jdbc_user" => @user,
     "jdbc_password" => @password.nil? ? nil : @password.value,
     "statement" => @statement,
     "parameters" => @parameters,
     "target" => @target
    )
    @jdbc_filter.register
  end # def register

  public
  def filter(event)
    @jdbc_filter.filter(event)
  end # def filter
end # class LogStash::Filters::JdbcMysql
