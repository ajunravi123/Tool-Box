from crewai import Agent, Task, Crew, LLM
# from crewai import Crew, Process, Task, Agent as CrewAgent, LLM



llm_client = LLM(
    model="gemini/gemini-2.0-flash",
    api_key="AIzaSyAgvxpCN-gwZKETeD8NavgeN94c11dw15U"
)






# --- Manager Agent (will delegate input) ---
manager_agent = Agent(
    role="Text Analysis Manager",
    goal="Orchestrate analysis of a given paragraph and return word and character counts with context.",
    backstory=(
        "You are a skilled AI project manager overseeing text analysis workflows. "
        "Your expertise lies in breaking down complex tasks, delegating them to the right specialists, "
        "and combining their outputs into a clean and actionable final result."
    ),
    llm=llm_client,
    allow_delegation=True,
    verbose=True
)

# --- Word Counter Agent ---
word_counter_agent = Agent(
    role="Word Counter",
    goal="Count the words in a sentence accurately and report with original sentence.",
    backstory=(
        "You specialize in identifying word boundaries and parsing text inputs, even in multilingual settings. "
        "Your output should be consistent and formatted clearly."
    ),
    llm=llm_client,
    allow_delegation=False,
    verbose=True
)

# --- Character Counter Agent ---
char_counter_agent = Agent(
    role="Character Counter",
    goal="Count the characters (including spaces and punctuation) in a sentence.",
    backstory=(
        "You're the go-to agent for character-level text analysis. "
        "You provide insights used in formatting, UI constraints, and linguistic breakdowns."
    ),
    llm=llm_client,
    allow_delegation=False,
    verbose=True
)

# --- Paragraph input (specified only in manager's task) ---
paragraph_input = "मैं हाल ही में अपनी भविष्य की संभावनाओं पर विचार कर रहा था।"

# --- Manager Task (delegates word/character counting) ---
manager_task = Task(
    description=(
        f"""
        Your job is to analyze the following paragraph:
        '{paragraph_input}'

        1. Ask the Word Counter to count the number of words and return it in the format:
           Word Count: <number> [<original sentence>]

        2. Ask the Character Counter to count the number of characters (including spaces and punctuation)
           and return it in the format:
           Character Count: <number> [<original sentence>]

        3. Combine both results into a final summary with each on its own line.
        """
    ),
    agent=manager_agent,
    expected_output="Word and character count, both shown with the original sentence."
)

# --- Crew with only manager task (delegates internally) ---
crew = Crew(
    agents=[manager_agent, word_counter_agent, char_counter_agent],
    tasks=[manager_task],
    manager=manager_agent,
    verbose=True
)

# --- Execute crew ---
final_output = crew.kickoff()
print("\n✅ Final Output:\n")
print(final_output)
