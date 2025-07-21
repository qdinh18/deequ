import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
os.environ['SPARK_VERSION'] = '3.5'
from pydeequ.analyzers import *
from pydeequ.checks import *
from pydeequ.verification import *
from pydeequ.profiles import *
from pydeequ.suggestions import *

# Initialize Spark Session with PyDeequ
spark = SparkSession.builder \
    .appName("PyDeequ-Data-Quality") \
    .master("local[*]") \
    .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.11-spark-3.5") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

class TransactionDataQualityPipeline:
    def __init__(self, data_path):
        self.data_path = data_path
        print(f"Loading data from: {data_path}")
        
        # Load the parquet file
        try:
            self.df = spark.read.parquet(data_path)
            print(f"Data loaded successfully!")
            print(f"Dataset shape: {self.df.count()} rows x {len(self.df.columns)} columns")
            
            # Display schema
            print("Dataset Schema:")
            self.df.printSchema()
            
            # Show sample data
            print("Sample Data (first 5 rows):")
            self.df.show(5, truncate=False)
            
        except Exception as e:
            print(f"Error loading data: {str(e)}")
            raise
        
    def run_profiling(self):
        """
        Component 1: PROFILES - Comprehensive data profiling
        """
        print("\n" + "="*60)
        print("COMPONENT 1: PROFILES - Data Profiling Analysis")
        print("="*60)
        
        # Get all columns for profiling
        all_columns = self.df.columns
        print(f"Profiling {len(all_columns)} columns: {', '.join(all_columns)}")
        
        column_profile_result = ColumnProfilerRunner(spark) \
            .onData(self.df) \
            .run()
        
        # Display comprehensive profiling results
        print("\n PROFILING RESULTS:")
        print("-" * 50)
        
        for column_name, profile in column_profile_result.profiles.items():
            print(f"\n Column: {column_name}")
            print(f"  Data Type: {profile.dataType}")
            print(f"  Completeness: {profile.completeness:.4f} ({profile.completeness*100:.1f}%)")
            print(f"  Distinct Values: ~{profile.approximateNumDistinctValues}")
            
        return column_profile_result
        
    def run_analyzers(self):
        """
        Component 2: ANALYZERS - Compute comprehensive metrics on data
        """
        print("\n" + "="*60)
        print(" COMPONENT 2: ANALYZERS - Computing Data Metrics")
        print("="*60)
        
        # Dynamically add analyzers based on data types
        analysis_runner = AnalysisRunner(spark).onData(self.df)
        
        # Add basic analyzers for all columns
        analysis_runner.addAnalyzer(Size())
        for col in self.df.columns:
            analysis_runner.addAnalyzer(Completeness(col))
            
        # Add type-specific analyzers
        for col, dtype in self.df.dtypes:
            if dtype in ['int', 'bigint', 'double', 'float']:
                analysis_runner.addAnalyzer(Mean(col))
                analysis_runner.addAnalyzer(StandardDeviation(col))
                analysis_runner.addAnalyzer(Minimum(col))
                analysis_runner.addAnalyzer(Maximum(col))
            elif dtype == 'string':
                analysis_runner.addAnalyzer(Distinctness(col))
                if col == "customer_email":
                    analysis_runner.addAnalyzer(PatternMatch(col, r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"))
        
        # Add special analyzers
        analysis_runner.addAnalyzer(Uniqueness(["transaction_id"]))
        analysis_runner.addAnalyzer(Entropy("payment_method"))
        analysis_runner.addAnalyzer(Entropy("customer_gender"))
        analysis_runner.addAnalyzer(Histogram("payment_method"))
        
        analysis_result = analysis_runner.run()

        analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysis_result)
            
        return analysisResult_df.show(truncate=False)
    
    def generate_suggestions(self):
        """
        Component 3: SUGGESTIONS - Generate constraint recommendations
        """
        print("\n" + "="*60)
        print("COMPONENT 3: SUGGESTIONS - Constraint Recommendations")
        print("="*60)
        
        suggestion_result = ConstraintSuggestionRunner(spark) \
            .onData(self.df) \
            .addConstraintRule(DEFAULT()) \
            .run()
        
        print("\n SUGGESTED CONSTRAINTS:")
        print("-" * 50)
        
        if suggestion_result:
            suggestion_result= spark.createDataFrame(suggestion_result['constraint_suggestions'])
        else:
            print("No constraint suggestions generated")
            
        return suggestion_result.show(truncate = False)
    
    def run_checks(self):
        """
        Component 4: CHECKS - Data validation with dynamic rules
        """
        print("\n" + "="*60)
        print(" COMPONENT 4: CHECKS - Data Validation")
        print("="*60)
        
        # Initialize check with basic rules
        check = Check(spark, CheckLevel.Error, "Dynamic Data Quality Check") \
            .hasSize(lambda x: x > 0, "Dataset must not be empty")
        
        # Add dynamic checks based on data types
        for col, dtype in self.df.dtypes:
            # Completeness check for all columns
            check = check.hasCompleteness(col, lambda x: x >= 0.8, f"{col} completeness")
            
            # Type-specific checks
            if dtype in ['int', 'bigint', 'double', 'float']:
                check = check.isNonNegative(col, f"{col} non-negative") \
                         .isComplete(col, f"{col} numeric completeness")
            elif dtype == 'string':
                check = check.isComplete(col, f"{col} string completeness")
                
                # Special pattern checks
                if col == "customer_email":
                    check = check.hasPattern(col, r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", 
                                           lambda x: x >= 0.8, "Email format")
                elif col == "customer_phone":
                    check = check.satisfies(f"length({col}) >= 10", "Phone length", lambda x: x >= 0.8)
        
        # Add business-specific rules
        check = check.isUnique("transaction_id", "Transaction ID uniqueness") \
                    .isComplete("customer_id", "Customer ID completeness") \
                    .isComplete("transaction_timestamp", "Timestamp completeness") \
                    .isNonNegative("unit_price", "Unit price non-negative") \
                    .isNonNegative("total_amount", "Total amount non-negative")
        
        # Execute verification
        verification_result = VerificationSuite(spark) \
            .onData(self.df) \
            .addCheck(check) \
            .run()
        
        # Convert to DataFrame for better visualization
        result_df = VerificationResult.checkResultsAsDataFrame(spark, verification_result)
        print("\n CHECK RESULTS:")
        result_df.show(n=result_df.count(), truncate=False)
        
        return verification_result
    
    def run_complete_pipeline(self):
        """
        Execute the complete PyDeequ data quality pipeline
        """
        print("\n STARTING COMPREHENSIVE DATA QUALITY ASSESSMENT")
        print("=" * 80)
        
        results = {}
        
        try:
            # Component 1: Profiling
            results['profiles'] = self.run_profiling()
            
            # Component 2: Analyzers
            results['analyzers'] = self.run_analyzers()
            
            # Component 3: Suggestions
            results['suggestions'] = self.generate_suggestions()
            
            # Component 4: Checks
            results['checks'] = self.run_checks()
            
            print("\n" + "="*80)
            print(" PIPELINE COMPLETED SUCCESSFULLY!")
            print("="*80)
            
            return results
            
        except Exception as e:
            print(f"\n PIPELINE FAILED: {str(e)}")
            raise

# Execute the pipeline
if __name__ == "__main__":
    data_path = "/Users/dinhvietquyen1803/Data_Engineering/dqops_lakehouse/airflow/data/very_dirty_data.parquet"
    
    print("Transaction Data Quality Pipeline")
    print("=" * 80)
    
    try:
        pipeline = TransactionDataQualityPipeline(data_path)
        results = pipeline.run_complete_pipeline()
        print("\n Analysis completed!")
        
    except Exception as e:
        print(f"\n Critical Error: {str(e)}")

    finally:
        spark.stop()
        print("\n Spark session closed")