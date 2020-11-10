#!/bin/bash
set -e
#
#if [ $# PIPELINEWISE_VENV 0 ]; then
#  echo "No arguments PIPELINEWISE_VENV supplied"
#fi
#
#if [ $PIPELINEWISE_HOME 0 ]; then
#  echo "No arguments PIPELINEWISE_HOME supplied"
#fi
#
#if [ $config_dir 0 ]; then
#  echo "No arguments config_dir supplied"
#fi
#
#if [ $tap_id 0 ]; then
#  echo "No arguments tap_id supplied"
#fi
#
#if [ $target_id 0 ]; then
#  echo "No arguments target_id supplied"
#fi

source $1 && export PIPELINEWISE_HOME=$2 && pipelinewise import --dir $3 && pipelinewise run_tap --tap $4 --target $5
exit
