from typing import Any, Dict, Optional

import pytest
from ruamel import yaml

import great_expectations as ge
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.rule_based_profiler.config.base import RuleBasedProfilerConfig
from great_expectations.rule_based_profiler.domain_builder import (
    DomainBuilder,
    SimpleColumnSuffixDomainBuilder,
)
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    DefaultExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    MetricMultiBatchParameterBuilder,
    NumericMetricRangeMultiBatchParameterBuilder,
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.rule.rule import Rule
from great_expectations.rule_based_profiler.rule_based_profiler import (
    BaseRuleBasedProfiler,
    RuleBasedProfiler,
)


@pytest.fixture
def data_context_with_taxi_data(empty_data_context):
    context: ge.DataContext = empty_data_context
    data_path: str = "/Users/work/Development/great_expectations/tests/test_sets/taxi_yellow_tripdata_samples/"

    datasource_config = {
        "name": "taxi_multibatch_datasource_other_possibility",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "PandasExecutionEngine",
        },
        "data_connectors": {
            "default_inferred_data_connector_name": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "base_directory": data_path,
                "default_regex": {
                    "group_names": ["data_asset_name", "month"],
                    "pattern": "(yellow_tripdata_sample_2018)-(\\d.*)\\.csv",
                },
            },
        },
    }

    context.test_yaml_config(yaml.dump(datasource_config))

    context.add_datasource(**datasource_config)
    return context


def test_domain_builder(data_context_with_taxi_data):
    context: ge.DataContext = data_context_with_taxi_data
    batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_multibatch_datasource_other_possibility",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="yellow_tripdata_sample_2018",
        data_connector_query={"index": -1},  # communicate this to users in a better way
    )
    domain_builder: DomainBuilder = SimpleColumnSuffixDomainBuilder(
        data_context=context,
        batch_request=batch_request,
        column_name_suffixes=["_amount"],
    )
    domains: list = domain_builder.get_domains()
    assert len(domains) == 4
    assert domains == [
        {"domain_type": "column", "domain_kwargs": {"column": "fare_amount"}},
        {"domain_type": "column", "domain_kwargs": {"column": "tip_amount"}},
        {"domain_type": "column", "domain_kwargs": {"column": "tolls_amount"}},
        {"domain_type": "column", "domain_kwargs": {"column": "total_amount"}},
    ]


def test_expectation_configuration_builder(data_context_with_taxi_data):
    # this is where you can really see the value of the RBP because you can calculate these values automatically
    default_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_not_be_null",
        column="total_amount",
    )
    # so this needs to be tested a bit better than before
    pass


def test_rule_workflow_add_rule(data_context_with_taxi_data):
    context: ge.DataContext = data_context_with_taxi_data
    batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_multibatch_datasource_other_possibility",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="yellow_tripdata_sample_2018",
        data_connector_query={"index": -1},
    )
    domain_builder: DomainBuilder = SimpleColumnSuffixDomainBuilder(
        data_context=context,
        batch_request=batch_request,
        column_name_suffixes=["_amount"],
    )
    default_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_not_be_null",
        column="$domain.domain_kwargs.column",
    )
    simple_variables_rule: Rule = Rule(
        name="rule_with_no_variables_no_parameters",
        domain_builder=domain_builder,
        expectation_configuration_builders=[default_expectation_configuration_builder],
    )
    # all of this is correct until this point.
    my_rbp: RuleBasedProfiler = RuleBasedProfiler(
        name="my_rbp", data_context=context, config_version=1.0
    )
    my_rbp.add_rule(rule=simple_variables_rule)
    res: ExpectationSuite = my_rbp.run()
    assert len(res.expectations) == 4


def test_add_rule_with_parameter_builder(data_context_with_taxi_data):
    context: ge.DataContext = data_context_with_taxi_data
    batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_multibatch_datasource_other_possibility",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="yellow_tripdata_sample_2018",
        data_connector_query={"index": -1},
    )
    domain_builder: DomainBuilder = SimpleColumnSuffixDomainBuilder(
        data_context=context,
        batch_request=batch_request,
        column_name_suffixes=["_amount"],
    )
    # parameter_builder
    numeric_range_parameter_builder: MetricMultiBatchParameterBuilder = (
        MetricMultiBatchParameterBuilder(
            data_context=context,
            batch_request=batch_request,
            metric_name="column.min",
            metric_domain_kwargs="$domain.domain_kwargs",
            name="my_column_min",
        )
    )

    config_builder: DefaultExpectationConfigurationBuilder = (
        DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_values_to_be_greater_than",
            value="$parameter.my_column_min.value[-1]",
            column="$domain.domain_kwargs.column",
        )
    )
    simple_variables_rule: Rule = Rule(
        name="rule_with_no_variables_no_parameters",
        domain_builder=domain_builder,
        parameter_builders=[numeric_range_parameter_builder],
        expectation_configuration_builders=[config_builder],
    )
    my_rbp = RuleBasedProfiler(name="my_rbp", data_context=context, config_version=1.0)
    my_rbp.add_rule(rule=simple_variables_rule)
    res: ExpectationSuite = my_rbp.run()
    assert len(res.expectations) == 4


def test_add_variables(data_context_with_taxi_data):
    context: ge.DataContext = data_context_with_taxi_data
    batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_multibatch_datasource_other_possibility",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="yellow_tripdata_sample_2018",
        data_connector_query={"index": -1},
    )
    domain_builder: DomainBuilder = SimpleColumnSuffixDomainBuilder(
        data_context=context,
        batch_request=batch_request,
        column_name_suffixes=["_amount"],
    )
    default_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_not_be_null",
        column="$domain.domain_kwargs.column",
    )
    simple_variables_rule: Rule = Rule(
        name="rule_with_parameters",
        domain_builder=domain_builder,
        expectation_configuration_builders=[default_expectation_configuration_builder],
    )
    # all of this is correct until this point.
    my_rbp: RuleBasedProfiler = RuleBasedProfiler(
        name="my_rbp", data_context=context, config_version=1.0
    )
    my_rbp.add_rule(rule=simple_variables_rule)
    res: ExpectationSuite = my_rbp.run()
    assert len(res.expectations) == 4

    # now adding parameter


def test_add_variables_currently_not_working(data_context_with_taxi_data):
    # how do variables work?
    # well we can update the config?
    context: ge.DataContext = data_context_with_taxi_data
    batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_multibatch_datasource_other_possibility",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="yellow_tripdata_sample_2018",
        data_connector_query={"index": -1},
    )
    # create a new RBP
    variables: Optional[Dict[str, Any]] = {"suffix": "_amount"}

    my_rbp = RuleBasedProfiler(
        name="my_rbp", data_context=context, config_version=1.0, variables=variables
    )
    domain_builder: DomainBuilder = SimpleColumnSuffixDomainBuilder(
        data_context=context,
        batch_request=batch_request,
        column_name_suffixes="$variables.suffix",
    )
    # parameter_builder
    numeric_range_parameter_builder: MetricMultiBatchParameterBuilder = (
        MetricMultiBatchParameterBuilder(
            data_context=context,
            batch_request=batch_request,
            metric_name="column.min",
            metric_domain_kwargs="$domain.domain_kwargs",
            name="my_column_min",
        )
    )
    config_builder: DefaultExpectationConfigurationBuilder = (
        DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_values_to_be_greater_than",
            value="$parameter.my_column_min.value[-1]",
            column="$domain.domain_kwargs.column",
        )
    )
    simple_variables_rule: Rule = Rule(
        name="rule_with_no_variables_no_parameters",
        domain_builder=domain_builder,
        parameter_builders=[numeric_range_parameter_builder],
        expectation_configuration_builders=[config_builder],
    )
    # do we need to update if the name is already in there?
    my_rbp.add_rule(rule=simple_variables_rule)
    res: ExpectationSuite = my_rbp.run()
    assert len(res.expectations) == 4


def test_rule_work_flow_with_save(data_context_with_taxi_data):
    #### NOTE : use the RECONCILIATION path to update the config.
    context: ge.DataContext = data_context_with_taxi_data
    batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_multibatch_datasource_other_possibility",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="yellow_tripdata_sample_2018",
        data_connector_query={"index": -1},
    )
    domain_builder: DomainBuilder = SimpleColumnSuffixDomainBuilder(
        data_context=context,
        batch_request=batch_request,
        column_name_suffixes=["_amount"],
    )
    # parameter_builder
    numeric_range_parameter_builder: MetricMultiBatchParameterBuilder = (
        MetricMultiBatchParameterBuilder(
            data_context=context,
            batch_request=batch_request,
            metric_name="column.min",
            metric_domain_kwargs="$domain.domain_kwargs",
            name="my_column_min",
        )
    )

    config_builder: DefaultExpectationConfigurationBuilder = (
        DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_values_to_be_greater_than",
            value="$parameter.my_column_min.value[-1]",
            column="$domain.domain_kwargs.column",
        )
    )
    simple_variables_rule: Rule = Rule(
        name="rule_with_no_variables_no_parameters",
        domain_builder=domain_builder,
        parameter_builders=[numeric_range_parameter_builder],
        expectation_configuration_builders=[config_builder],
    )
    my_rbp = RuleBasedProfiler(name="my_rbp", data_context=context, config_version=1.0)
    res = my_rbp.to_json_dict()
    assert res == {
        "class_name": "RuleBasedProfiler",
        "module_name": "great_expectations.rule_based_profiler.rule_based_profiler",
        "name": "my_rbp",
        "rules": [],
        "variables": {"parameter_nodes": None},
    }
    my_rbp.add_rule(rule=simple_variables_rule)
    my_rbp.save_config()
    res = my_rbp.config.to_json_dict()
    assert res == {
        "variables": {"parameter_nodes": None},
        "rules": {
            "rule_with_no_variables_no_parameters": {
                "domain_builder": {
                    "class_name": "SimpleColumnSuffixDomainBuilder",
                    "column_name_suffixes": ["_amount"],
                    "batch_request": {
                        "datasource_name": "taxi_multibatch_datasource_other_possibility",
                        "data_connector_name": "default_inferred_data_connector_name",
                        "data_asset_name": "yellow_tripdata_sample_2018",
                        "batch_spec_passthrough": None,
                        "data_connector_query": {"index": -1},
                        "limit": None,
                    },
                    "module_name": "great_expectations.rule_based_profiler.domain_builder.simple_column_suffix_domain_builder",
                },
                "parameter_builders": [
                    {
                        "metric_domain_kwargs": "$domain.domain_kwargs",
                        "class_name": "MetricMultiBatchParameterBuilder",
                        "replace_nan_with_zero": False,
                        "metric_value_kwargs": None,
                        "batch_request": {
                            "datasource_name": "taxi_multibatch_datasource_other_possibility",
                            "data_connector_name": "default_inferred_data_connector_name",
                            "data_asset_name": "yellow_tripdata_sample_2018",
                            "batch_spec_passthrough": None,
                            "data_connector_query": {"index": -1},
                            "limit": None,
                        },
                        "enforce_numeric_metric": False,
                        "metric_name": "column.min",
                        "reduce_scalar_metric": True,
                        "module_name": "great_expectations.rule_based_profiler.parameter_builder.metric_multi_batch_parameter_builder",
                        "name": "my_column_min",
                    }
                ],
                "expectation_configuration_builders": [
                    {
                        "expectation_type": "expect_column_values_to_be_greater_than",
                        "class_name": "DefaultExpectationConfigurationBuilder",
                        "condition": None,
                        "column": "$domain.domain_kwargs.column",
                        "meta": {},
                        "value": "$parameter.my_column_min.value[-1]",
                        "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder.default_expectation_configuration_builder",
                    }
                ],
            }
        },
        "config_version": 1.0,
        "name": "my_rbp",
    }


def test_with_multi_batch_quentin_workflow(data_context_with_taxi_data):

    #### NOTE : use the RECONCILIATION path to update the config.
    context: ge.DataContext = data_context_with_taxi_data
    batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_multibatch_datasource_other_possibility",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="yellow_tripdata_sample_2018",
        data_connector_query={"index": -1},
    )
    domain_builder: DomainBuilder = SimpleColumnSuffixDomainBuilder(
        data_context=context,
        batch_request=batch_request,
        column_name_suffixes=["_amount"],
    )
    # parameter_builder
    numeric_range_parameter_builder: NumericMetricRangeMultiBatchParameterBuilder = (
        NumericMetricRangeMultiBatchParameterBuilder(
            data_context=context,
            batch_request=batch_request,
            metric_name="column.quantile_values",
            metric_domain_kwargs="$domain.domain_kwargs",
            metric_value_kwargs={
                "quantiles": [2.5e-1, 5.0e-1, 7.5e-1],
                "allow_relative_error": "linear",
            },
            false_positive_rate="5.0e-2",
            num_bootstrap_samples="9139",
            name="quantile_value_ranges",
        )
    )
    config_builder: DefaultExpectationConfigurationBuilder = (
        DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_quantile_values_to_be_between",
            column="$domain.domain_kwargs.column",
            quantile_ranges={
                "quantiles": [2.5e-1, 5.0e-1, 7.5e-1],
                "value_ranges": "$parameter.quantile_value_ranges.value.value_range",
            },
            allow_relative_error="linear",
            meta={"profiler_details": "$parameter.quantile_value_ranges.details"},
        )
    )
    simple_variables_rule: Rule = Rule(
        name="rule_with_no_variables_no_parameters",
        domain_builder=domain_builder,
        parameter_builders=[numeric_range_parameter_builder],
        expectation_configuration_builders=[config_builder],
    )
    my_rbp = RuleBasedProfiler(name="my_rbp", data_context=context, config_version=1.0)
    my_rbp.add_rule(rule=simple_variables_rule)
    res = my_rbp.run()
    print(res)
    print("this worked my friends")


def test_with_add_parameter(data_context_with_taxi_data):
    #### NOTE : use the RECONCILIATION path to update the config.
    context: ge.DataContext = data_context_with_taxi_data
    batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_multibatch_datasource_other_possibility",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="yellow_tripdata_sample_2018",
        data_connector_query={"index": -1},
    )


def test_add_parameter_update_rule():
    pass
