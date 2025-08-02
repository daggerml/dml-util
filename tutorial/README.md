# DaggerML Tutorials

Welcome to the comprehensive DaggerML tutorial series! These interactive Jupyter notebooks will guide you through mastering DaggerML, from basic concepts to production-ready workflows.

## Overview

DaggerML is a framework for building reproducible computational workflows using DAGs (Directed Acyclic Graphs). These tutorials provide hands-on experience with real examples and best practices.

## Tutorial Series

### 📚 [Tutorial 1: Getting Started](01-getting-started.ipynb)
**Duration:** ~30 minutes  
**Prerequisites:** None

Learn the fundamentals:
- Creating DaggerML instances and DAGs
- Working with literal values and nodes
- Understanding the difference between nodes and values
- Basic function creation with `@funkify`
- Function arguments and DAG inspection

**Key Concepts:** DAGs, Nodes, Functions, Basic Operations

---

### 🔗 [Tutorial 2: Function Composition and Caching](02-function-composition-and-caching.ipynb)
**Duration:** ~45 minutes  
**Prerequisites:** Tutorial 1

Master advanced workflows:
- Chaining functions together
- Understanding DaggerML's caching system
- Parallel processing patterns
- Complex data structure handling
- Performance optimization through caching

**Key Concepts:** Function Chaining, Caching, Parallel Processing, Performance

---

### 🛡️ [Tutorial 3: Error Handling and External Data](03-error-handling-and-external-data.ipynb)
**Duration:** ~40 minutes  
**Prerequisites:** Tutorials 1-2

Build robust pipelines:
- Error handling patterns in DaggerML
- Working with external files (JSON, CSV)
- Data validation and parsing
- Robust data loading with fallbacks
- Error recovery strategies

**Key Concepts:** Error Handling, External Data, Validation, Robustness

---

### ⚡ [Tutorial 4: Storage, Environments, and Scaling](04-storage-environments-and-scaling.ipynb)
**Duration:** ~50 minutes  
**Prerequisites:** Tutorials 1-3

Production-ready workflows:
- S3Store for artifact management
- Different execution environments (local, container, cloud)
- Function adaptation patterns
- Scaling strategies and resource management
- Real-world pipeline design

**Key Concepts:** Storage, Environments, Scaling, Production Patterns

## Prerequisites

### Required
- Python 3.8+
- Jupyter Notebook or JupyterLab
- Basic Python programming knowledge

### Installation
```bash
# Install DaggerML and utilities
pip install dml-util[dml]

# Or install individually
pip install daggerml daggerml-cli dml-util
```

### Optional (for Tutorial 4)
- Docker (for container execution examples)
- AWS credentials (for S3Store examples)

## Getting Started

1. **Clone or download** the tutorial notebooks
2. **Install dependencies** using pip
3. **Start Jupyter** in the tutorial directory:
   ```bash
   jupyter lab
   # or
   jupyter notebook
   ```
4. **Open Tutorial 1** and begin your DaggerML journey!

## Setup Instructions

### Basic Setup
```bash
# Create a new repository for your tutorials
dml repo create tutorial

# Create cache directory
dml cache create

# Set your username
dml config user $USER

# Set the repository
dml config repo tutorial
```

### Advanced Setup (Optional)
For cloud features in Tutorial 4:

```bash
# Set up S3 storage (requires AWS credentials)
export DML_S3_BUCKET=your-bucket-name
export DML_S3_PREFIX=dml-tutorials

# Configure AWS credentials
aws configure
```

## Tutorial Structure

Each tutorial follows a consistent structure:

1. **Introduction** - Overview and learning objectives
2. **Prerequisites** - What you should know before starting
3. **Hands-on Examples** - Interactive code cells with explanations
4. **Concepts** - Detailed explanations of key ideas
5. **Best Practices** - Production-ready patterns and recommendations
6. **Summary** - What you've learned and next steps

## Learning Path

### Beginner Path
Complete tutorials 1-2 to get comfortable with basic DaggerML concepts and workflows.

### Intermediate Path  
Complete tutorials 1-3 to learn error handling and external data integration.

### Advanced Path
Complete all tutorials 1-4 to master production-ready DaggerML workflows.

## Tips for Success

### 🎯 **Run Every Code Cell**
The tutorials are designed to be interactive. Run each code cell to see the results and understand the concepts.

### 🔍 **Experiment**
Try modifying the code examples! Change parameters, add new functions, or create variations to deepen your understanding.

### 📝 **Take Notes**
Add markdown cells with your own observations and insights as you work through the tutorials.

### 🤝 **Ask Questions**
If something isn't clear, create GitHub issues or reach out to the DaggerML community.

### 🔄 **Review and Practice**
Come back to earlier tutorials to reinforce concepts as you learn more advanced topics.

## Common Issues and Solutions

### Environment Setup
- **Issue:** "Module not found" errors
- **Solution:** Ensure all packages are installed: `pip install dml-util[dml]`

### DaggerML Configuration
- **Issue:** Repository or cache errors
- **Solution:** Run the setup commands above to configure DaggerML properly

### Jupyter Issues
- **Issue:** Kernel crashes or cells don't execute
- **Solution:** Restart the kernel (Kernel → Restart) and re-run cells from the beginning

### AWS/S3 Issues (Tutorial 4)
- **Issue:** S3Store examples fail
- **Solution:** These are optional. You can learn the concepts without actual AWS setup

## What You'll Build

By the end of these tutorials, you'll have built:

### Tutorial 1 Projects
- ✅ Basic data processing DAG
- ✅ Simple statistical analysis functions
- ✅ Understanding of node vs value concepts

### Tutorial 2 Projects
- ✅ Multi-stage data processing pipeline
- ✅ Parallel processing workflows
- ✅ Complex nested data structures
- ✅ Performance-optimized functions with caching

### Tutorial 3 Projects
- ✅ Error-resilient data processing pipeline
- ✅ External file integration (JSON, CSV)
- ✅ Robust data loading with fallbacks
- ✅ Cross-source data analysis

### Tutorial 4 Projects
- ✅ Production-ready data pipeline
- ✅ Environment-adaptive functions
- ✅ Resource optimization strategies
- ✅ Scalable workflow architecture

## Advanced Topics

After completing these tutorials, explore:

- **Custom Adapters**: Build your own execution environments
- **DAG Optimization**: Advanced caching and performance strategies  
- **Production Deployment**: Scaling DaggerML in production
- **Integration Patterns**: Connecting DaggerML with other tools

## Community and Support

- 📖 **Documentation**: [DaggerML Docs](https://github.com/daggerml/python-lib)
- 💬 **Discussions**: GitHub Discussions for questions and community
- 🐛 **Issues**: GitHub Issues for bug reports and feature requests
- 🔗 **Examples**: Additional examples in the main repositories

## Contributing

Found an issue or have an improvement idea?

1. **Open an issue** describing the problem or enhancement
2. **Submit a pull request** with your changes
3. **Improve documentation** by clarifying confusing sections
4. **Share your own examples** and use cases

## License

These tutorials are distributed under the MIT License, same as DaggerML.

---

## Quick Reference

### Key DaggerML Concepts
- **DAG**: Directed Acyclic Graph representing your computation
- **Node**: Reference to data or computation results
- **Function**: Reusable computation decorated with `@funkify`
- **Caching**: Automatic memoization based on inputs
- **Adapter**: Execution environment configuration

### Essential Commands
```python
# Create DaggerML instance
dml = Dml(repo="my_repo", branch="main")

# Create new DAG
dag = dml.new("dag_name", "description")

# Create function
@funkify
def my_function(dag):
    result = process(dag.argv[1].value())
    dag.result = result
    return dag.result

# Add function to DAG
dag.my_fn = my_function
dag.result = dag.my_fn(input_data)

# Get actual value
value = dag.result.value()
```

Happy learning! 🚀✨
