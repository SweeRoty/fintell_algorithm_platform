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
select task in "Centrality: degree" "Centrality: closeness" "Centrality: PageRank"
do
	case $task in
		"Centrality: degree" )
			path=analysis/centrality/
			algo=degree.gsql
			dest=output\\/$g_name\\/degree_dist.csv;;
		"Centrality: closeness" )
			read -p "Please enter the maximum iteration for closeness computation (e.g. 10): " max_iter
			path=analysis/centrality/
			algo=closeness.gsql
			dest=output\\/$g_name\\/closeness_dist.csv;;
		"Centrality: PageRank" )
			read -p "Please enter the tolerance to declare convergence (e.g. 0.001)" tol
			read -p "Please enter the maximum iteration for PageRank algorithm (e.g. 10): " max_iter
			read -p "Please enter the damping factor for PageRank algorithm (e.g. 0.85): " damping
			path=analysis/centrality/
			algo=pagerank.gsql
			dest=output\\/$g_name\\/pagerank_score.csv
	esac
	cp $path$algo tmp_script.gsql
	sed -i "s/G_NAME/$g_name/g" tmp_script.gsql
	sed -i "s/V_NAME/$v_name/g" tmp_script.gsql
	sed -i "s/E_NAME/$e_name/g" tmp_script.gsql
	sed -i "s/OUTPUT/$dest/g" tmp_script.gsql
	case $task in
		"Centrality: closeness" )
			max_iter=$((max_iter+0))
			sed -i "s/MAX_ITER/$max_iter/g" tmp_script.gsql;;
		"Centrality: PageRank" )
			tol=$(bc -l <<<"${tol}")
			max_iter=$((max_iter+0))
			damping=$(bc -l <<<"${damping}")
			sed -i "s/TOL/$tol/g" tmp_script.gsql
			sed -i "s/MAX_ITER/$max_iter/g" tmp_script.gsql
			sed -i "s/DAMPING/$damping/g" tmp_script.gsql
	esac
	gsql tmp_script.gsql
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
