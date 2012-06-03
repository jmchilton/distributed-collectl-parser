# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant::Config.run do |config|
  # All Vagrant configuration is done here. The most common configuration
  # options are documented and commented below. For a complete reference,
  # please see the online documentation at vagrantup.com.

  # vagrant box add lucid64 http://files.vagrantup.com/lucid64.box
  config.vm.box = "lucid64"
  config.vm.network :hostonly, "33.33.33.11"
  config.vm.forward_port 80, 9080
  
end
