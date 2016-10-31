node /^master/ {
  include role::master
}

node /^slave/ {
  include role::slave
}
