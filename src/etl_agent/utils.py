"""

Utility functions for the agent loop.

"""
import re
from abc import ABC, abstractmethod
from collections import namedtuple
from e2b_code_interpreter import Sandbox
from together import Together


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


# We use the TogetherCodeExecutor sandbox class implementation to run code. You can implement your own 
# executor if you want to use a different provider / method to run code.  
class TogetherCodeExecutor(CodeExecutor):    
    
    def __init__(self, envs: list = None):
        client = Together()
        self.envs = envs if envs else {}
        self.code_interpreter = client.code_interpreter 

    def run_code(self, code: str, python_packages: list = None) -> ExecutorResponse:
        # pre-pend the code with the necessary imports if needed
        imports = [f"!pip install {p}" for p in python_packages] if python_packages else []
        final_code = f"{'\n'.join(imports)}\n{code}" if imports else code
        # we pass the key as file to the code interpreter: note - this is a workaround
        # waiting for secret management support in the code interpreter
        file_to_upload = {
            "name": "bauplan_apikey.txt",
            "encoding": "string",
            "content": self.envs.get('BAUPLAN_API_KEY', '')
        }
        response = self.code_interpreter.run(code=final_code, language="python", files=[file_to_upload])

        st_out = []
        st_err = []
        results = []
        for output in response.data.outputs:
            if output.type == "stdout":
                line = f"[Code Interpreter] {output.data}"
                print(line)
                st_out.append(line)
            elif output.type == "stderr":
                line = f"[Code Interpreter] {output.data}"
                print(line)
                st_err.append(line)
            elif output.type == "execute_result":
                line = f"Result: {output.data}"
                print(line)
                results.append(line)
                
        if response.data.errors:
            st_err.append(f"[Code Interpreter] Errors: {response.data.errors}")
        
        # Return the results in a structured format
        return ExecutorResponse(
            result='\n'.join(results),
            stdout='\n'.join(st_out),
            stderr='\n'.join(st_err),
            error=response.data.errors if response.data.errors else None
        )

 
class E2BCodeExecutor(CodeExecutor):    
    
    def __init__(self, api_key, envs: list = None):
        self.sbx = Sandbox(api_key=api_key, envs=envs) 
        
    def run_code(self, code: str, python_packages: list = None) -> ExecutorResponse:
        with self.sbx:
            # Install any required Python packages
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