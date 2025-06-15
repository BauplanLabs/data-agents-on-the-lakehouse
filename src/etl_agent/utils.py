"""

Utility functions for the agent loop.

"""
import re
from abc import ABC, abstractmethod
from collections import namedtuple
from e2b_code_interpreter import Sandbox

# structure to hold parsed response components
ParsedResponse = namedtuple('ParsedResponse', ['done', 'packages', 'reasoning', 'code'])


def parse_response(response_text: str) -> ParsedResponse:
    """
    Parse the LLM response and extract reasoning, code, or answer sections: thanks Fede!
    
    We return a structured response with the following fields:
    - done: A boolean indicating if the task is complete (if <done> tag is present)
    - packages: The list of packages to install if present
    - reasoning: The reasoning section if present
    - code: The code section if present
    
    If the response does not conform to the expected format, we raise an error.
    """
    
    packages = None # this should be a list of packages to install
    reasoning = None
    code = None
    done = False
    
    # Check for <done> tag indicating completion
    if "<done>" in response_text:
        done = True
    
    # Check for packages
    if "<packages>" in response_text and '</packages>' in response_text:
        p_match = re.search(r"<packages>(.*?)</packages>", response_text, re.DOTALL)
        if p_match:
            packages = p_match.group(1).strip().split(',')
    
    # Check for reasoning and code sections
    if "<reasoning>" in response_text and '</reasoning>' in response_text and "<code>" in response_text and '</code>' in response_text:
        reasoning_match = re.search(r"<reasoning>(.*?)</reasoning>", response_text, re.DOTALL)
        code_match = re.search(r"<code>(.*?)</code>", response_text, re.DOTALL)
        
        if reasoning_match and code_match:
            reasoning = reasoning_match.group(1).strip()
            code = code_match.group(1).strip()
            
    assert done or (reasoning is not None and code is not None), f"Response was not valid: {response_text}\n"

    return ParsedResponse(done=done, packages=packages, reasoning=reasoning, code=code)


# structure to hold the result of code execution
ExecutorResponse = namedtuple('ExecutorResponse', ['result', 'stdout', 'stderr', 'error'])

# abstract base class for code execution
class CodeExecutor(ABC):
    
    @abstractmethod
    def run_code(self, code: str, python_packages: list = None):
        pass

# We use the E2B Code Interpreter class implementation to run code. You can implement your own 
# executor if you want to use a different provider / method to run code.  
class E2BCodeExecutor(CodeExecutor):    
    
    def __init__(self, api_key, envs: list = None):
        self.sbx = Sandbox(api_key=api_key, envs=envs) 
        
    def run_code(self, code: str, python_packages: list = None) -> ExecutorResponse:
        with self.sbx:
            # Install any required Python packages
            # Note that for example in Together AI sandbox you would pre-prend to the code
            # the line `!pip install <package>` to install packages...
            if python_packages:
                for pkg in python_packages:
                    self.sbx.commands.run(f"pip install {pkg}")
            # Run the provided code with stream handlers for stdout and stderr
            exec = self.sbx.run_code(code,
                on_stderr=lambda stderr: print("[Code Interpreter]", stderr),
                on_stdout=lambda stdout: print("[Code Interpreter]", stdout)
            )
            # Return the results in a structured format
            return ExecutorResponse(
                result=exec.results,
                stdout=exec.logs.stdout,
                stderr=exec.logs.stderr,
                error=exec.error
            )