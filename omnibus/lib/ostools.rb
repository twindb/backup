# ------------------------------------
# OS-detection helper functions
# ------------------------------------
def linux?()
    return %w(rhel debian fedora suse gentoo slackware arch exherbo).include? ohai['platform_family']
end

def redhat?()
    return %w(rhel fedora).include? ohai['platform_family']
end

def centos?()
  return ohai['platform'] == "centos"
end

def debian?()
    return ohai['platform_family'] == 'debian'
end

def focal?()
    if ohai['platform'] == 'ubuntu'
        return ohai['lsb']['codename'] == 'focal'
    end
    return false
end

def bionic?()
    if ohai['platform'] == 'ubuntu'
        return ohai['lsb']['codename'] == 'bionic'
    end
    return false
end

def osx?()
    return ohai['platform_family'] == 'mac_os_x'
end

def windows?()
    return ohai['platform_family'] == 'windows'
end

def suse?()
  print ohai["platform_family"]
  return ohai["platform_family"] == 'suse'
end
