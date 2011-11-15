#!/bin/bash

FREE_AUTOREM=`echo no | apt-get autoremove | grep freed | awk '{ print $4 }'`;





