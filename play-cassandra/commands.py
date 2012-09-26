# Here you can create play commands that are specific to the module, and extend existing commands
import os
import subprocess

MODULE = 'cassandra'

# Commands that are specific to your module

COMMANDS = ['cassandra:schema', 'cassandra:apply', 'cassandra:tempbalances', 'cassandra:tempcreate']

def execute(**kargs):
    command = kargs.get("command")
    app = kargs.get("app")
    args = kargs.get("args")
    env = kargs.get("env")

    if command == "cassandra:schema":
        print "~ Generating Cassandra schema from the database objects"
        print "~ "
        java_cmd = app.java_cmd([], None, "play.modules.cassandra.util.SchemaExtractor", args)
        try:
            subprocess.call(java_cmd, env=os.environ)
        except OSError:
            print "Could not execute the java executable, please make sure the JAVA_HOME environment variable is set properly (the java executable should reside at JAVA_HOME/bin/java). "
            sys.exit(-1)
    elif command == "cassandra:tempbalances":
        print "~ Listing all account balances"
        print "~ "
        java_cmd = app.java_cmd([], None, "play.modules.cassandra.util.temp.AccountBalances", args)
        try:
            subprocess.call(java_cmd, env=os.environ)
        except OSError:
            print "Could not execute the java executable, please make sure the JAVA_HOME environment variable is set properly (the java executable should reside at JAVA_HOME/bin/java). "
            sys.exit(-1)
    elif command == "cassandra:tempcreate":
        print "~ Listing creation date for all accounts"
        print "~ "
        java_cmd = app.java_cmd([], None, "play.modules.cassandra.util.temp.AccountRegister", args)
        try:
            subprocess.call(java_cmd, env=os.environ)
        except OSError:
            print "Could not execute the java executable, please make sure the JAVA_HOME environment variable is set properly (the java executable should reside at JAVA_HOME/bin/java). "
            sys.exit(-1)
    elif command == "cassandra:apply":
        print "~ Updating Fixtures in the Cassandra cluster"
        print "~ "
        java_cmd = app.java_cmd([], None, "play.modules.cassandra.util.FixtureWriter", args)
        try:
            subprocess.call(java_cmd, env=os.environ)
        except OSError:
            print "Could not execute the java executable, please make sure the JAVA_HOME environment variable is set properly (the java executable should reside at JAVA_HOME/bin/java). "
            sys.exit(-1)

# This will be executed before any command (new, run...)
def before(**kargs):
    command = kargs.get("command")
    app = kargs.get("app")
    args = kargs.get("args")
    env = kargs.get("env")


# This will be executed after any command (new, run...)
def after(**kargs):
    command = kargs.get("command")
    app = kargs.get("app")
    args = kargs.get("args")
    env = kargs.get("env")

    if command == "new":
        pass
