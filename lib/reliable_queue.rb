%w( queue_manager object ).each {|f|
  require File.join(File.dirname(__FILE__), 'reliable_queue', f)
}
# vim: filetype=ruby sw=2 sts=2 ts=4 expandtab :
