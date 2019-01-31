# Spark Tuner: Developmemt

## Dev notes

### Setup

See also
* [spark-install](https://github.com/zipfian/spark-install)
* [Spark+ipython_on_MacOS.md](https://gist.github.com/ololobus/4c221a0891775eaa86b0)

#### Mac

Assuming `brew` is installed:

* Install XCode: `xcode-select --install`
* Install Java: `brew tap caskroom/versions && brew cask install java8`
* Run `java -version` to ensure Java 1.8

To install Apache Spark:

* `brew install apache-spark`

Update system environment variables with:

```
export SPARK_HOME=`brew info apache-spark | grep /usr | tail -n 1 | cut -f 1 -d " "`/libexec
```

### Creating a Pull Request

To create a pull request:
1. Create a new branch and commit changes
2. Push branch to remote
3. Click the "Pull Request" button on the repo page and choose your branch
4. Fill out the relevant information and click "Send pull request". Your PR can now be reviewed by other contributers
5. Once approved, click "Merge pull request," which allows you to add a commit message. You will need to deal with any merge conflicts.

There are more detailed instructions for navigating GitHub's pull request system [here](https://yangsu.github.io/pull-request-tutorial/).

## TODO

#### Objectives:
* Tune Spark configuration parameters in a hands-off manner
* Learn from tuning experiences over time to:
    * Tune more efficiently over time,
    * Answer counterfactual questions about application performance, and
    * Suggest interventions to improve application performance (potentially even 
      code changes or environment updates apart from configuration setting).

#### High-level directions:
* Optimization: which algorithms are most effective for tuning Spark, and why? 
  The meta question is that of Optimal design, and selection of appropriate objective. 
  Some possible algos:
    * Zero-gradient technique e.g. Random Nelder-Mead
    * Multi-armed bandit
    * Branch-and-bound sampling e.g. Latin Hypercube sampling
    * Bayesian optimization
    * Control-theoretic techniques e.g. Kalman filtering
* Learning: is there a way to encode information about Spark parameter’s effect on 
  runtime/resources for a fixed app?
    * Want to be able to ask counterfactual questions about parameters, code change, 
      or data size change.
* Extensions: 
    * What other RM frameworks, apart from YARN, should this work on?
    * What kind of UI might be useful? Job submission framework?
    * What kind of backend service would be useful?
    * Should we allow running tests in parallel? What changes would be required?
    * What kind of metrics should be stored, and where?
* Externalities: 
    * How can cluster issues be accounted for?
    * How can code issues be accounted for using the query plan and configuration 
      information?

#### Urgent
* Fix `sort` to write to local filesystem by default. See 
  [this](https://stackoverflow.com/questions/27299923/how-to-load-local-file-in-sc-textfile-instead-of-hdfs)

#### Next steps
* Finish the new objective function that, over _similar_ values of run-time, minimizes
  resource usage. E.g. if `spark.default.parallelism` ranging from 1 to 10 yields the 
  same runtime in all cases, the optimal configuration value should be 1.
  * Add tests for spark_metrics
  * Finally, allow different types of objective functions
  * Record metrics somewhere. Ideally, we'd also periodically record objects returned
  by Cluster Metrics API and Cluster Scheduler API to track externalities.
* Reduce build time on Travis by 50%. 
* List options in README.md?
* Put tests_require=test_requires, in setup.py. requirements.txt shouldn't contain
  test packages.
* Rethink how FIXED_SPARK_PARAM interact with the configurable param, esp whether
  or not they are merged. Don't want issues due to duplicates. Maybe this should
  come from SparkSubmitCmd defaults. Some notion of defaults and overrides.
* The underlying optimization also doesn't seem to terminate if the objective
  value doesn't change over successive iterations.
* Allow JAR parameters to also be configurable.  
* Figure out some DoWhy basics. 
  * In particular, figure out a sensible causal graph for Spark parameters.

#### Nice to have
* Use dependency injection where possible for resuability
* Generate Spinx docs, and update comments to link to external docs. 
* Tuner as a service. Opentuner issue [25](https://github.com/jansel/opentuner/issues/25).
* New objective that optimizes the construction of a causal graph.
* Ideally, all tuning run details are published to a central location as a time-series.
  This would help pool run data, and track of significant outcome changes (e.g. as a result
  of a code change). Even better if they can be easily visualized (see e.g. 
[opentuner-visualizer](https://github.com/danula/opentuner-visualizer)).
* The default parameters should be read from a file instead of being hard-coded. All
  parameters should be merged into a final map.
* Since Spark parameters are many and jobs can take a while to run, warm starting
  autotuning might be useful.
* Need to validate all Spark parameters.

## References

### Auto-tuning

Research:
* OpenTuner: An Extensible Framework for Program Autotuning 
[paper](groups.csail.mit.edu/commit/papers/2014/ansel-pact14-opentuner.pdf),
[code](https://github.com/jansel/opentuner)
* BOAT: Building Auto-Tuners with Structured Bayesian Optimization 
[paper](https://www.cl.cam.ac.uk/~mks40/pubs/www_2017.pdf), 
[code](https://github.com/VDalibard/BOAT)
* Learning Runtime Parameters in Computer Systems with Delayed Experience Injection 
[paper](https://www.cl.cam.ac.uk/~mks40/pubs/nips_drl_2016.pdf)
* Datasize-Aware High Dimensional Configurations Auto-Tuning of In-Memory Cluster Computing
[paper](alchem.usc.edu/portal/static/download/dac.pdf)
* BestConfig: Tapping the Performance Potential of Systems via Automatic Configuration Tuning
[paper](https://arxiv.org/abs/1710.03439),
[code](https://github.com/zhuyuqing/bestconf)
* AutoConfig: Automatic Configuration Tuning for Distributed Message Systems
[paper](web.cs.ucdavis.edu/~liu/paper/ASE18Bao.pdf),
[code](https://github.com/sselab/autoconfig)
* Autotuning research at LBL CRD 
[link](http://crd.lbl.gov/departments/computer-science/PAR/research/autotuning/)
* Design continuums 
[blog](https://blog.acolyer.org/2019/01/21/design-continuums-and-the-path-toward-self-designing-key-value-stores-that-know-and-learn/),
[paper](https://stratos.seas.harvard.edu/publications/design-continuums-and-path-toward-self-designing-key-value-stores-know-and)
* Spark Autotuning 
[talk](https://databricks.com/session/spark-autotuning)

TODO: Comparison between different approaches

####  OpenTuner 

##### Creating an OpenTuner configuration

Implement [MeasurementInterface](https://github.com/jansel/opentuner/blob/c9db469889b9b504d1f7affe2374b2750adafe88/opentuner/measurement/interface.py),
three methods in particular:
* `manipulator`: defines the search space across configuration parameters
* `run`: runs a program for given configuration and returns the result
* `save_final_config`: saves optimal configuration, after tuning, to a file

A fourth, `objective`, should be implemented if an objective function other than the default
[MinimizeTime](https://github.com/jansel/opentuner/blob/c9db469889b9b504d1f7affe2374b2750adafe88/opentuner/search/objective.py)
is desired.

### Spark, Hadoop, YARN 

#### Spark 

##### Spark configuration parameters 

Note that some commonly used Spark (2.0.2+) parameters:

|Parameter Name|Direct?|Sample Value|
|--------------|-------|------------|
|master|Yes|yarn|
|deploy-mode|Yes|cluster|
|driver-memory|Yes|10G|
|executor-memory|Yes|20G|
|executor-cores|Yes|4|
|spark.default.parallelism|No|100|
|spark.dynamicAllocation.enabled|No|true|
|spark.dynamicAllocation.maxExecutors|No|10|
|spark.eventLog.enabled|No|true|
|spark.eventLog.dir|No|hdfs:///var/log/spark/apps|
|spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version|No|2|
|spark.shuffle.service.enabled|No|true|
|spark.sql.autoBroadcastJoinThreshold|No|-1|
|spark.sql.shuffle.partitions|No|100|
|spark.yarn.maxAppAttempts|No|1|
|spark.memory.fraction|No|0.6|
|spark.memory.storageFraction|No|0.5|

For a larger set, see 
[this](https://spark.apache.org/docs/2.4.0/configuration.html).

##### Spark performance tuning

* [sparklens](https://github.com/umayrh/sparklens)
* [dr-elephant](https://github.com/linkedin/dr-elephant)
* [sparklint](https://github.com/groupon/sparklint)
* [sparkMeasure](https://github.com/LucaCanali/sparkMeasure)


* A Methodology for Spark Parameter Tuning [link](delab.csd.auth.gr/papers/BDR2017gt.pdf)

#### YARN

Resources:
* [Overview](https://hortonworks.com/apache/yarn/), 
  [architecture](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html), and
  [documentation](http://hadoop.apache.org/docs/current/)
* [YARN REST API](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/WebServicesIntro.html)
* Running Spark on YARN [link](https://spark.apache.org/docs/latest/running-on-yarn.html) 

### Performance measurement

#### Process statistics

* [psutil](https://psutil.readthedocs.io)
* [psrecord](https://github.com/astrofrog/psrecord)
* [Total memory used](https://stackoverflow.com/questions/938733/total-memory-used-by-python-process)
