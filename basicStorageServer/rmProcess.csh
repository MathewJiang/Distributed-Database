#!/bin/csh

ps aux | egrep server_ | awk '{print $2}' | xargs kill
