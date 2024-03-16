#! /usr/bin/bash

echo -n "Enter an integer: "
read n1
echo -n "Enter another integer: "
read n2

sum=$(($n1+$n2))
product=$(($n1*$n2))

echo "The sum of $n1 and $n2 is $sum"
echo "The product of $n1 and $n2 is $product."

if [ $sum -lt $product ]
then
  echo "The sum is less than the product."
elif [[ $sum == $product ]]
then
  echo "The sum is equal to the product."
elif [ $sum -gt $product ]
then
  echo "The sum is greater than the product."
fi