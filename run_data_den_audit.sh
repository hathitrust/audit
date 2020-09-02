#!/bin/bash

root="${1:-/htdataden/}"

export HTFEED_CONFIG=$SDRROOT/feed/etc/config_dev.yaml
export FEED_HOME=$SDRROOT/feed
export PERL5LIB=$FEED_HOME/lib:$FEED_HOME/metslib:$FEED_HOME/google/lib
perl audit_data_den.pl $root