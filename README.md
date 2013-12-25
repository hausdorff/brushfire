Brushfire
=========

avi@avibryant.com

Brushfire is a framework for distributed supervised learning of decision tree models using Scalding and Algebird.

The basic approach to distributed tree learning is inspired by Google's PLANET, but considerably generalized thanks to Scala's type parameterization and Algebird's algebraic abstractions.

**Both the framework and these docs are woefully incomplete, but hopefully give a flavor of how it works (or is meant to work).**

## Inputs to the framework

Building a tree requires the following things:

- Training data, represented by maps of features to values and associated labels. The only restriction is that features must have an ordering (so that they can be used as keys during map/reduce). In Scala terms, something like:

````scala
def trainingData[K:Ordering,V,L] : TypedPipe[(Map[K,V],L)]
````

- A Learner that can produce and aggregate summary statistics for a feature given a value and a label, and can then produce candidate segmentations ("splits") based on those summary stats, and a numeric score (eg, a p-value) for how useful those splits are. In Scala terms:

````scala
case class Split[V,O](goodness : Double, predicates : Iterable[(V=>Boolean,O)])

trait Learner[V,L,S,O]
  def statsSemigroup : Semigroup[S]

  def buildStats(value : V, label : L) : S
  def findSplits(stats : S) : Iterable[Split[V,O]]
}
````

That's a lot of type parameters, so let's summarize:

- K:Ordering - Identifies features in the model. Example: String
- V - Values of features. Example: Double
- L - Labels we're trying to predict. Example: Boolean
- S:Semigroup - Aggregate stats mapping values to labels for a single feature. Generally some kind of approximation of the joint distribution of the feature values and the label. Example: Map[Double,Map[Boolean,Long]]
- O - Label predictor. Generally some kind of approximation of the distribution of the label. Example: Map[Boolean,Double]

One big weakness to note in this model is that it assumes that for a given job, all features have the same value type, V (and similarly that they get with the same stats type, S). Since you often want models with mixed feature types (some numeric and some categorical, say), in practice V and S will probably be abstract roots of case class hierarchies which wrap multiple "real" value types. That's annoying, but it's not clear how to make Scala happy any other way.

## Under the hood

Trees are built breadth first, one level at a time. Building each level uses two map reduce steps:

- First, map over every row of the training data. Using the tree that's been built so far, find the leaf node for that row. For each feature, build a summary stats object using the feature value and the row's label. Emit (leaf,feature)->stats pairs.
- In the combine/reduce phase, group and sum the stats for each (leaf,feature).
- Now, from the aggregated stats, find all the candidate splits for each (leaf,feature). Emit (leaf,split) pairs.
- In the combine/reduce phase, just pick the best split for each leaf. Most of that can happen on the map side, so it's cheap to use a single reducer which will end up with the best split for all leaves. This reducer can create a new level for the tree based on those splits.

## TODO

- [ ] Stop criteria, and/or pruning; not all leaves should be split all the time.
- [ ] Cross-validation. There's a tiny "Scorer" trait as a brief sketch of this but nothing more.
- [ ] Lots more example learners and building blocks for them, eg, using Count-Min Sketch for large categorical features, or QTree to avoid having to quantize numerical features, or using CHAID-style multiway splits, etc etc.
- [ ] Better output of finished trees, in a way that makes them actually usable by other code.

