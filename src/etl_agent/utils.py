"""

Utility functions for the agent loop.

"""
import re
from abc import ABC, abstractmethod
from collections import namedtuple
from e2b_code_interpreter import Sandbox

# structure to hold parsed response components
ParsedResponse = namedtuple('ParsedResponse', ['answer', 'reasoning', 'code'])


def parse_response(response_text: str) -> ParsedResponse:
    """
    Parse the LLM response and extract reasoning, code, or answer sections: thanks Fede!
    
    We return a structured response with the following fields:
    - answer: The final answer if present
    - reasoning: The reasoning section if present
    - code: The code section if present
    
    If the response does not conform to the expected format, we raise an error.
    """
    
    final_answer = None
    reasoning = None
    code = None
    # Check for final answer
    if "<answer>" in response_text and "</answer>" in response_text:
        answer_match = re.search(r"<answer>(.*?)</answer>", response_text, re.DOTALL)
        if answer_match:
            final_answer = answer_match.group(1).strip()
    
    # Check for reasoning and code sections
    if "<reasoning>" in response_text and "</reasoning>" in response_text and "<code>" in response_text and "</code>" in response_text:
        reasoning_match = re.search(r"<reasoning>(.*?)</reasoning>", response_text, re.DOTALL)
        code_match = re.search(r"<code>(.*?)</code>", response_text, re.DOTALL)
        
        if reasoning_match and code_match:
            reasoning = reasoning_match.group(1).strip()
            code = code_match.group(1).strip()
            
    assert (reasoning is not None and code is not None) or final_answer is not None, \
        f"Response was not valid: {response_text}\n"

    return ParsedResponse(answer=final_answer, reasoning=reasoning, code=code)


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
    
    def __init__(self, api_key):
        self.sbx = Sandbox(api_key=api_key) 
        
    def run_code(self, code: str, python_packages: list = None):
        with self.sbx:
            # Install any required Python packages
            # Note that for example in Together AI sandbox you would pre-prend to the code
            # the line `!pip install <package>` to install packages...
            if python_packages:
                for pkg in python_packages:
                    self.sbx.commands.run(f"pip install {pkg}")
            # Run the provided code with stream handlers for stdout and stderr
            exec = self.sbx.commands.run_code(code,
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