# Running a pipeline - Tutorial

Before beginning this tutorial, ensure that `cgat-core` is installed correctly. Refer to the [installation instructions](#installation) for guidance.

As a tutorial example of how to run a CGAT workflow, we will use the `cgat-showcase` pipeline. You will also need to install `cgat-showcase` (see the [instructions](https://cgat-showcase.readthedocs.io/en/latest/getting_started/Tutorial.html)).

The aim of this pipeline is to perform pseudoalignment using `kallisto`. The pipeline can be run locally or distributed across a cluster. This tutorial will explain the steps required to run the pipeline. Further documentation on `cgat-showcase` can be found [here](https://cgat-showcase.readthedocs.io/en/latest/).

The `cgat-showcase` pipeline highlights some of the functionality of `cgat-core`. Additionally, more advanced workflows for next-generation sequencing analysis are available in the [cgat-flow repository](https://github.com/cgat-developers/cgat-flow).

## Tutorial start

### Step 1: Download the tutorial data

Create a new directory, navigate to it, and download the test data:

```bash
mkdir showcase
cd showcase
wget https://www.cgat.org/downloads/public/showcase/showcase_test_data.tar.gz
tar -zxvf showcase_test_data.tar.gz
```

### Step 2: Generate a configuration YAML file

Navigate to the test data directory and generate a configuration file for the pipeline:

```bash
cd showcase_test_data
cgatshowcase transdiffexpres config
```

Alternatively, you can call the workflow file directly:

```bash
python /path/to/file/pipeline_transdiffexpres.py config
```

This will generate a `pipeline.yml` file containing configuration parameters that can be used to modify the pipeline output. For this tutorial, you do not need to modify the parameters to run the pipeline. In the [Modify Config](#modify-config) section below, you will find details on how to adjust the config file to change the pipeline's output.

### Step 3: Run the pipeline

To run the pipeline, execute the following command in the directory containing the `pipeline.yml` file:

```bash
cgatshowcase transdiffexpres make full -v5 --no-cluster
```

The `--no-cluster` flag will run the pipeline locally if you do not have access to a cluster. If you have access to a cluster, you can remove the `--no-cluster` option, and the pipeline will distribute the jobs across the cluster.

**Note**: There are many command line options available to run the pipeline. To see the available options, run:

```bash
cgatshowcase --help
```

This will start the pipeline execution. Monitor the output for any errors or warnings.

### Step 4: Review Results

Once the pipeline completes, review the output files generated in the `showcase_test_data` directory. These files contain the results of the pseudoalignment.

### Troubleshooting

- **Common Issues**: If you encounter errors during execution, ensure that all dependencies are installed and paths are correctly set.
- **Logs**: Check the log files generated during the pipeline run for detailed error messages.
- **Support**: For further assistance, refer to the [CGAT-core documentation](https://cgat-core.readthedocs.io/en/latest/) or raise an issue on our [GitHub repository](https://github.com/cgat-developers/cgat-core/issues).

### Step 5: Generate a report

The final step is to generate a report to display the output of the pipeline. We recommend using `MultiQC` for generating reports from commonly used bioinformatics tools (such as mappers and pseudoaligners) and `Rmarkdown` for generating custom reports.

To generate these reports, run the following command:

```bash
cgatshowcase transdiffexprs make build_report -v 5 --no-cluster
```

This will generate a `MultiQC` report in the folder `MultiQC_report.dir/` and an `Rmarkdown` report in `R_report.dir/`.

## Conclusion

This completes the tutorial for running the `transdiffexprs` pipeline for `cgat-showcase`. We hope you find it as useful as we do for writing workflows in Python.