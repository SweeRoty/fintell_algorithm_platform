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

curr_dir=`pwd`
dest_dir=$curr_dir/output/$g_name/
mkdir -p $dest_dir

echo "Please select the task: "
select task in "Centrality: degree" "Centrality: closeness" "Centrality: PageRank" "Local Clustering Coefficient" "Connected Components" "Community: LPA" "Community: Louvain"
do
	case $task in
		"Centrality: degree" )
			path=graph_analysis/centrality/
			algo=degree.gsql
			dest=${dest_dir}degree_dist.csv;;
		"Centrality: closeness" )
			read -p "Please enter the maximum iteration for closeness computation (e.g. 10): " max_iter
			path=graph_analysis/centrality/
			algo=closeness.gsql
			dest=${dest_dir}closeness_dist.csv;;
		"Centrality: PageRank" )
			read -p "Please enter the tolerance to declare convergence (e.g. 0.001)" tol
			read -p "Please enter the maximum iteration for PageRank algorithm (e.g. 10): " max_iter
			read -p "Please enter the damping factor for PageRank algorithm (e.g. 0.85): " damping
			path=graph_analysis/centrality/
			algo=pagerank.gsql
			dest=${dest_dir}pagerank_score.csv;;
		"Local Clustering Coefficient" )
			path=graph_analysis/clustering/
			algo=clustering_coef.gsql
			dest=${dest_dir}local_clustering_coef.csv;;
		"Connected Components" )
			path=graph_analysis/components/
			algo=conn_comp.gsql
			dest=${dest_dir}conn_comp_size.csv;;
		"Community: LPA" )
			read -p "Please enter the maximum iteration for label propagation (e.g. 50): " max_iter
			path=graph_mining/community_detection/
			algo=lpa.gsql
			dest=${dest_dir}lpa_communities.csv;;
		"Community: Louvain" )
			read -p "Please enter the maximum iteration for 1st phase (e.g. 20)" iter1
			read -p "Please enter the maximum iteration for 2nd phase (e.g. 10)" iter2
			path=graph_mining/community_detection/
			algo=louvain.gsql
			dest=${dest_dir}louvain_communities.csv
	esac
	cp $path$algo tmp_script.gsql
	sed -i "s/G_NAME/$g_name/g" tmp_script.gsql
	sed -i "s/V_NAME/$v_name/g" tmp_script.gsql
	sed -i "s/E_NAME/$e_name/g" tmp_script.gsql
	sed -i "s?OUTPUT?$dest?g" tmp_script.gsql
	case $task in
		"Centrality: closeness" )
			max_iter=$((max_iter))
			sed -i "s/MAX_ITER/$max_iter/g" tmp_script.gsql;;
		"Centrality: PageRank" )
			tol=$(bc -l <<<"${tol}")
			max_iter=$((max_iter+0))
			damping=$(bc -l <<<"${damping}")
			sed -i "s/TOL/$tol/g" tmp_script.gsql
			sed -i "s/MAX_ITER/$max_iter/g" tmp_script.gsql
			sed -i "s/DAMPING/$damping/g" tmp_script.gsql;;
		"Community: LPA" )
			max_iter=$((max_iter))
			sed -i "s/MAX_ITER/$max_iter/g" tmp_script.gsql;;
		"Community: Louvain" )
			iter1=$((iter1))
			iter2=$((iter2))
			sed -i "s/ITER1/$iter1/g" tmp_script.gsql
			sed -i "s/ITER2/$iter2/g" tmp_script.gsql
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
