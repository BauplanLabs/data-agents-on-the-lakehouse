"""

The main ETL agent loop, running basically a ReAct pattern to execute code and 
fix its own mistakes based on the output of executing the actual code.

"""


import os
from litellm import completion
from utils import parse_response, ParsedResponse, E2BCodeExecutor
from dotenv import load_dotenv
from prompts import SYSTEM_PROMPT
load_dotenv()
# check the basic envs are here, Bauplan, Together API, E2B
assert 'BAUPLAN_API_KEY' in os.environ, "BAUPLAN_API_KEY not set in environment variables"
assert 'E2B_API_KEY' in os.environ, "E2B_API_KEY not set in environment variables"
assert 'TOGETHER_API_KEY' in os.environ, "TOGETHER_API_KEY not set in environment variables"
# you could also use OpenAI API key and check you have it set in the environment variables here
# assert 'OPENAI_API_KEY'


def run_react_loop(
    user_input: str, 
    together_api_key: str,
    bauplan_api_key: str,
    eb2_api_key: str,
    system_prompt: str,
    max_iterations: int, 
    verbose: bool = False
):
    """
        Run the main ReAct reasoning and acting loop
    """
    history = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_input}
    ]
    
    print("🚀 Starting the Agent")
    print(f"📝 Task: {user_input}")
    print("="*50)
    
    current_iteration = 0
    while current_iteration < max_iterations:
        try:
            print("\n Getting LLM completion...")
            # 1. Get LLM response
            response = completion(
                # change here to use OpenAI models if you prefer
                # but make sure the relevant environment variable is set
                model="together_ai/deepseek-ai/DeepSeek-V3",
                messages=history,
                max_tokens=1000,
                temperature=0.7
            )            
            response_text = response.choices[0].message.content
            if verbose:
                print(f"\n🤖 Iteration {current_iteration}, LLM text:\n{response_text}")
                
            # 2. Parse response
            response: ParsedResponse = parse_response(response_text)
            if response.answer is not None:
                # We have a final answer, exit the loop
                if verbose:
                    print(f"\n🎯 Final Answer:\n{response.answer}")
                break
            
            # 3. We have a reasoning and code to execute
            if verbose:
                print(f"\n🤔 Reasoning: {response.reasoning}")
                print(f"\n🛠️ Code: {response.code}")
            
            # Execute the code in a fresh sandbox environment
            # replace this with your own code executor or implement something like 
            # a factory pattern to use different executors
            print("\n Running the code...")
            executor = E2BCodeExecutor(api_key=os.environ['E2B_API_KEY'])
            execution_result = executor.run_code(code=response.code)
            if verbose:
                print(f"\n📊 Result: {execution_result}")
            # Add to conversation history
            history.append({"role": "assistant", "content": response_text})
            history.append({"role": "user", "content": f"Observation: {execution_result}"})
            
            current_iteration += 1
            print("\n" + "-"*50)
            
        except Exception as e:
            print(f"Error in iteration {current_iteration}: {e}")
            break
    
    if current_iteration >= max_iterations:
        print(f"\n⚠️ Reached maximum iterations ({max_iterations})")


if __name__ == "__main__":
    question = """
         I am using bauplan and I need to load the parquet files in this bucket, with public reads available: s3://public-bucket.  Documentation for bauplan is available at online: you can use web tools to find what you are looking for.  I need to use the write-audit-publish (WAP) pattern, to safely import the data into tables - one file per table -  and I need to run a quality check at the end of the import. Use the concepts and APIs specific to bauplan to accomplish this:  for example, bauplan might support concepts like namespaces, clones, data branches etc., which are some of the ways WAP is done.  You do NOT have to use all of these concepts, but you can use them if they are available in the platform.  If a table already exists, you can replace it using the platform-specific APIs, like deletion, replacement etc. At the end of the import, use the platform APIs to run a query and check that the id column in every table just imported have no null values.  Generate a Python script that uses the secrets  to access bauplan. Make sure to catch any exceptions and print them to the console so that we can debug the code if it fails.  If you need to use any other libraries, make sure to import them as well.  Do NOT output anything that is not the Python code itself. 
    """
    run_react_loop(
        user_input=question,
        together_api_key=os.environ['TOGETHER_API_KEY'],
        bauplan_api_key=os.environ['BAUPLAN_API_KEY'],
        eb2_api_key=os.environ['E2B_API_KEY'],
        system_prompt=SYSTEM_PROMPT,
        max_iterations=5,
        verbose=True
    )