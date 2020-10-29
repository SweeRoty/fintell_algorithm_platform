#!/bin/bash

read -p "Please enter the graph name to be analyzed: " g_name
flag=`gsql "show graph $g_name"`
if [[ "$flag" ==  "Semantic Check Fails: The graph $g_name doesn't exist!" ]]
then
	echo "Graph $g_name does not exist in the database"
	exit
fi
read -p "Please enter the vertex name of this graph to be analyzed: " v_name
if [[ "$flag" != *"$v_name:v,"* ]]
then
	echo "Graph $g_name does not contain Vertex $v_name"
	exit
fi
read -p "Please enter the edge name of this graph to be analyzed: " e_name
if [[ "$flag" != *"$e_name:e,"* ]] && [[ "$flag" != *"$e_name:e)"* ]];
then
	echo "Graph $g_name does not contain Edge $e_name"
	exit
fi

mkdir -p output/$g_name

echo "Please select the task: "
select task in "Centrality: degree" "Centrality: closeness"
do
	case $task in
		"Centrality: degree" )
			path=analysis/centrality/
			algo=degree.gsql
			dest=output\\/$g_name\\/degree_dist.csv;;
		"Centrality: closeness" )
			read -p "Please enter the maximum iteration for closeness computation (default is 10): " max_iter
			path=analysis/centrality/
			algo=closeness.gsql
			dest=output\\/$g_name\\/closeness_dist.csv
	esac
	cp $path$algo tmp_script.gsql
	sed -i "s/G_NAME/$g_name/g" tmp_script.gsql
	sed -i "s/V_NAME/$v_name/g" tmp_script.gsql
	sed -i "s/E_NAME/$e_name/g" tmp_script.gsql
	sed -i "s/OUTPUT/$dest/g" tmp_script.gsql
	case $task in
		"Centrality: closeness" )
			max_iter=$((max_iter+0))
			sed -i "s/MAX_ITER/$max_iter/g" tmp_script.gsql
	esac
	echo "Continue or not? "
	select flag in "YES" "NO"
	do
		case $flag in
			"YES" )
				echo "Please select the listed task to continue"
				break;;
			"NO" )
				exit
		esac
		break
	done
done