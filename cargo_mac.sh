#!/bin/bash

# Since openssl libraries are no longer included in Mac OS X, we install
# openssl with homebrew, and export the necessary environments for cargo to
# build openssl properly.
#
# Run this script like you would run cargo

export OPENSSL_INCLUDE_DIR=/usr/local/opt/openssl/include
export OPENSSL_ROOT_DIR=/usr/local/opt/openssl

cargo $@
