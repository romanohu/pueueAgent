#!/usr/bin/env bash
for i in 1 2 3; do echo "step $i loss 0.$((10 - i))"; sleep 1; done
echo "final accuracy 0.95"
