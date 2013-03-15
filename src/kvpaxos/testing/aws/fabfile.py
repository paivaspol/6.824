from __future__ import with_statement
from fabric.api import run, env, local, parallel
from fabric.context_managers import cd
from fabric.contrib.console import confirm

#'ec2-54-234-223-49.compute-1.amazonaws.com'
env.hosts = ['ec2-54-234-252-48.compute-1.amazonaws.com', 'ec2-50-17-105-37.compute-1.amazonaws.com', 'ec2-54-242-171-198.compute-1.amazonaws.com', 'ec2-54-242-213-128.compute-1.amazonaws.com']
env.user = 'ubuntu'
env.forward_agent = True

@parallel
def git_pull():
	run("git clone git@github.com:dghubble/6.824-labs.git && ls")
	position_for_test()

@parallel
def git_update():
	run("cd 6.824-labs && git pull origin master")
	position_for_test()

def position_for_test():
	run("cd 6.824-labs/src && export GOPATH=$HOME/6.824-labs && cd kvpaxos && go test -i")

@parallel
def simple_test():
	run("cd 6.824-labs/src && export GOPATH=$HOME/6.824-labs && cd kvpaxos && go test -i && go test > debug.log 2> error.log")

@parallel
def medium_round():
	run("cd 6.824-labs/src && export GOPATH=$HOME/6.824-labs && cd kvpaxos && go test -i && bash repeat_test.sh > test/test.log")
