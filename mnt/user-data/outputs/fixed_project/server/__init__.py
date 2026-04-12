try:
    from .pipeline_environment import PipelineEnvironment
except ImportError:
    from server.pipeline_environment import PipelineEnvironment
