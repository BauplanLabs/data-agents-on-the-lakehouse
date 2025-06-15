"""

The main ETL agent loop, running basically a ReAct pattern to execute code and 
fix its own mistakes based on the output of executing the actual code.

"""


import os
from litellm import completion
from utils import parse_response, ParsedResponse, E2BCodeExecutor, ExecutorResponse
from dotenv import load_dotenv
from verifier import verify_etl_process
from prompts import SYSTEM_PROMPT, USER_PROMPT_TEMPLATE
load_dotenv()
# check the basic envs are here, Bauplan, Together API, E2B
assert 'BAUPLAN_API_KEY' in os.environ, "BAUPLAN_API_KEY not set in environment variables"
assert 'E2B_API_KEY' in os.environ, "E2B_API_KEY not set in environment variables"
assert 'TOGETHER_API_KEY' in os.environ, "TOGETHER_API_KEY not set in environment variables"
# model specific "global" variables
# you can change them or abstract them away in a config file
MAX_TOKENS = 1000
TEMPERATURE = 0.7
MAX_ITERATIONS = 5


def run_react_loop(
    templated_user_input: str, 
    s3_raw_bucket: str,
    bauplan_api_key: str,
    model_name: str,
    max_tokens: int,
    temperature: float,
    eb2_api_key: str,
    system_prompt: str,
    max_iterations: int, 
    llm_folder: str,
    verbose: bool = False
):
    """
        Run the main ReAct reasoning and acting loop: we return the final answer
        if we have one, or None if we reach the maximum number of iterations without a valid answer.
        
        Parameters:
        - templated_user_input: A string template for the user input, which will be formatted with the S3 bucket name.
        - s3_raw_bucket: The S3 bucket name where the raw data is stored.
        - bauplan_api_key: The API key for the Bauplan client.
        - model_name: The name of the LLM model to use for completions.
        - max_tokens: The maximum number of tokens for the LLM response.
        - temperature: The temperature for the LLM response.
        - eb2_api_key: The API key for the E2B Code Interpreter service.
        - system_prompt: The system prompt to guide the agent's behavior.
        - max_iterations: The maximum number of iterations to run the loop.
        - llm_folder: The folder where the generated code will be stored for human inspection.
        - verbose: Whether to print detailed logs during execution.
    """
    # start the message history with the system prompt and user input
    user_input = templated_user_input.format(
        s3_raw_bucket=s3_raw_bucket,
    )
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
                # change here to use OpenAI models, but make sure the relevant env is set
                # by default, we use Together AI: note we checked at the start
                # that the relevant env is set
                model=model_name,
                messages=history,
                max_tokens=max_tokens,
                temperature=temperature
            )            
            response_text = response.choices[0].message.content
            # 2. Parse response
            response: ParsedResponse = parse_response(response_text)
            if response.done:
                print(f"\n✅ Done! Final result: {response_text}")
                # we are done, return the final answer
                return response_text
            
            if verbose:
                print(f"\nIteration {current_iteration + 1} response:")
                print(f"\n📦 Packages to install: {response.packages}")
                print(f"\n🤔 Reasoning: {response.reasoning}")
                print(f"\n🛠️ Code: {response.code[:500]}")
            
            # Store the code in a file for human inspection
            code_file_path = os.path.join(llm_folder, 'etl_agent', f"iteration_{current_iteration}_code.py")
            with open(code_file_path, 'w') as code_file:
                code_file.write(response.code)
                
            # 3. Execute the code
            # Replace this with your own code executor or implement something like 
            # a factory pattern to use different executors
            
            print("\n Running the code...")
            executor = E2BCodeExecutor(
                api_key=eb2_api_key, 
                envs={'BAUPLAN_API_KEY': bauplan_api_key}
            )
            execution_result: ExecutorResponse = executor.run_code(
                code=response.code, 
                python_packages=response.packages
            )
            if verbose:
                print(f"\n📊 Result: {execution_result}")
            # If we are not done, we add the response to the conversation history
            content = f"Result:{execution_result.result} \nStandard output: {execution_result.stdout} \nError output if any: {execution_result.stderr} \nSandbox error if any: {execution_result.error}"
            history.append({"role": "assistant", "content": response_text})
            history.append({"role": "user", "content": content})
            # Go to the next iteration
            current_iteration += 1
            print("\n" + "-"*50)
            
        except Exception as e:
            print(f"Error in iteration {current_iteration}: {e}")
            break
    
    if current_iteration >= max_iterations:
        print(f"\n⚠️ Reached maximum iterations ({max_iterations})")
        
    # we failed, return None
    return None


if __name__ == "__main__":
    # the llm_code folder is used for human inspection of the code generated by the agents
    llm_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), "llm_code")
    
    # As mentioned in the blog post, an alternative setup would be to use a
    # a loop over models here (for model in models_list) and run more than one agent
    # over the lakehouse, sandboxed using data branches - the verifier would then
    # check + merge the most promising results from the different agents.
    # We leave this extension as an exercise for the reader ;-)
    answer = run_react_loop(
        templated_user_input=USER_PROMPT_TEMPLATE,
        s3_raw_bucket=os.environ['S3_BUCKET_RAW_DATA'],
        model_name="together_ai/deepseek-ai/DeepSeek-V3",
        bauplan_api_key=os.environ['BAUPLAN_API_KEY'],
        eb2_api_key=os.environ['E2B_API_KEY'],
        system_prompt=SYSTEM_PROMPT,
        max_iterations=MAX_ITERATIONS,
        max_tokens=MAX_TOKENS,
        temperature=TEMPERATURE,
        llm_folder=llm_folder,
        verbose=True
    )
    if answer is None:
        print("❌ Failed to get a valid answer from the agent.")
    else:
        # if we have an answer, we can run the human-in-the-loop verifier function
        if verify_etl_process():
            print("✅ ETL process verified successfully, we can go to the next step!")
        else:
            print("❌ ETL process verification failed.")