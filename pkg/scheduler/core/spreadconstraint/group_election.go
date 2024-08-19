/*
Copyright 2022 The Karmada Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package spreadconstraint

import (
	"errors"
	"fmt"
	"sort"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
)

// Elect method to select clusters based on the required replicas
func (root *groupRoot) Elect() ([]*clusterv1alpha1.Cluster, error) {
	election, err := root.selectCluster(root.Replicas)
	if err != nil {
		return nil, err
	} else if root.Replicas > 0 && election.Replicas < root.Replicas {
		return nil, fmt.Errorf("no enough resource when selecting %d clusters", len(election.Clusters))
	}
	return toCluster(sortClusters(election.Clusters)), nil
}

// selectCluster method to select clusters based on the required replicas
func (node *groupNode) selectCluster(replicas int32) (*candidate, error) {
	if node.Leaf {
		return node.selectByClusters(replicas)
	}
	return node.selectByGroups(replicas)
}

// selectByClusters selects clusters from the current group's Clusters list
func (node *groupNode) selectByClusters(replicas int32) (*candidate, error) {
	result := &candidate{
		Name: node.Name,
	}
	for _, cluster := range node.Clusters {
		if replicas <= 0 || cluster.AvailableReplicas > 0 {
			result.Clusters = append(result.Clusters, cluster)
		}
	}
	result.MaxScore = node.MaxScore
	result.Replicas = node.AvailableReplicas
	return result, nil
}

// selectByGroups selects clusters from the sub-groups
func (node *groupNode) selectByGroups(replicas int32) (*candidate, error) {
	if !node.Valid {
		return nil, errors.New("the number of feasible clusters is less than spreadConstraint.MinGroups")
	}
	// use DFS to find the best path
	paths := node.findPaths(replicas)
	if len(paths) == 0 {
		return nil, fmt.Errorf("no enough resource when selecting %d %ss", node.MaxGroups, node.Constraint)
	}

	var results []*candidate
	for _, path := range paths {
		// TODO optimize
		var candidates []*candidate
		for _, node := range path.Nodes {
			candidate, _ := node.Group.selectCluster(ternary(replicas <= 0, replicas, replicas-path.Replicas+node.Replicas))
			candidates = append(candidates, candidate)
		}
		candidate := node.merge(candidates)
		candidate.Id = path.Id
		candidate.Name = node.Name
		results = append(results, candidate)
	}

	return node.selectBest(results), nil
}

// findPaths finds all possible paths of groups that meet the required replicas using DFS.
func (node *groupNode) findPaths(replicas int32) (paths []*dfsPath) {
	current := &dfsPath{}
	// recursively constructs paths of groups to meet the required replicas.
	var dfsFunc func(int, *dfsPath)
	dfsFunc = func(start int, current *dfsPath) {
		length := current.length()
		if replicas <= 0 && length == node.MaxGroups ||
			(replicas > 0 && current.Replicas >= replicas && length > 0 &&
				(node.MinGroups <= 0 || length >= node.MinGroups)) {
			paths = append(paths, current.next())
		} else if length < node.MaxGroups {
			for i := start; i < len(node.Groups); i++ {
				if node.Groups[i].Valid {
					current.addLast(node.Groups[i])
					dfsFunc(i+1, current)
					current.removeLast()
				}
			}
		}
	}
	dfsFunc(0, current)
	return paths
}

// selectBest selects the best candidates from the given candidates based on the maximum score and replicas.
func (node *groupNode) selectBest(candidates []*candidate) *candidate {
	size := len(candidates)
	if size == 0 {
		return nil
	} else if size > 1 {
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].MaxScore != candidates[j].MaxScore {
				return candidates[i].MaxScore > candidates[j].MaxScore
			} else if candidates[i].Replicas != candidates[j].Replicas {
				return candidates[i].Replicas > candidates[j].Replicas
			}
			return candidates[i].Id < candidates[j].Id
		})
	}
	return candidates[0]
}

// merge combines a list of candidate objects into a single candidate.
// It merges the clusters, updates the maximum score, and sums the replicas.
func (node *groupNode) merge(candidates []*candidate) *candidate {
	size := len(candidates)
	maxScore := int64(0)
	replicas := int32(0)
	var clusters []*clusterDesc
	if size == 1 {
		maxScore = candidates[0].MaxScore
		replicas = candidates[0].Replicas
		clusters = candidates[0].Clusters
	} else {
		maps := make(map[string]bool, size)
		for _, candidate := range candidates {
			for _, cluster := range candidate.Clusters {
				if _, ok := maps[cluster.Name]; !ok {
					maps[cluster.Name] = true
					clusters = append(clusters, cluster)
					replicas += cluster.AvailableReplicas
					if cluster.Score > maxScore {
						maxScore = cluster.Score
					}
				}
			}
		}
	}
	return &candidate{
		Name:     node.Name,
		MaxScore: maxScore,
		Replicas: replicas,
		Clusters: clusters,
	}
}
