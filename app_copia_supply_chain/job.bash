#!/bin/bash
# Batch Processing Script for Supply Chain

echo "Usage:"
echo "A. Enter start and end dates."
echo "B. Choose command to process:"
echo "1 - Confirmed Sale Orders"
echo "2 - Requisitions vs Purchases"
echo "3 - Purchases vs Planned Inbounds"
echo "4 - Available Qty vs Outbound"
echo "5 - Delivery Manifest"
echo "6 - Delivery Returns"
echo "7 - Historical QoH"

echo ":> Please enter a start date: "
read input_start
echo ":> Please enter an end date: "
read input_end
echo ":> Please enter a command to process: "
read input_command

startdate=$(date -I -d "$input_start") || exit -1
enddate=$(date -I -d "$input_end")     || exit -1

if [[ "$(date -d "$startdate" +%Y%m%d)" -gt "$(date -d "$enddate" +%Y%m%d)" ]]; then
  echo "Date mismatch!"
  exit
fi

if [[ $input_command -gt 7 ]]; then
  echo "Bad command, GT!"
  exit
fi

if [[ $input_command -lt 1 ]]; then
  echo "Bad command, LT!"
  exit
fi

d="$startdate"
while [ "$d" != "$enddate" ]; do
  echo $d
  if [[ $input_command == 1 ]]; then
    python application.py today_confirmed_order_lines $d
  elif [[ $input_command == 2 ]]; then
    python application.py requisition_vs_purchase $d
  elif [[ $input_command == 3 ]]; then
    python application.py purchase_planned_inbound $d
  elif [[ $input_command == 4 ]]; then
    python application.py available_qty_outbound $d
  elif [[ $input_command == 5 ]]; then
    python application.py delivery_manifest $d
  elif [[ $input_command == 6 ]]; then
    python application.py delivery_returns $d
  elif [[ $input_command == 7 ]]; then
    python application.py historical_qoh $d
  else
    echo "Bad command!"
  fi
  d=$(date -I -d "$d + 1 day")
done
