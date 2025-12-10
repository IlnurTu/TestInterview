#!/bin/bash
[ "$1" = "with_timer" ] && TIMER_FLAG="-DwithTimer" || TIMER_FLAG=""
g++ -std=c++17 -O3 -march=native $TIMER_FLAG test_task.cc -o word_counter && echo "Собрано!"