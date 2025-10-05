import os
import sys
import json
from dotenv import load_dotenv
from datasets import Dataset
from ragas import evaluate
from ragas.metrics import (
    faithfulness,
    answer_relevancy,
    context_recall,
    context_precision,
)

# --- Setup System Path ---
# This finds the project root by going up four directories and adds it to the path
project_root = os.path.dirname(os.path.dirname(os.path.dirname((os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.append(project_root)


# --- Project-specific Imports ---
try:
    from app.database.pg_vector import PGVectorManager
    # LangChain components for RAG
    from langchain_openai import ChatOpenAI
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.runnables import RunnableLambda, RunnablePassthrough, RunnableParallel
except ImportError as e:
    print(f"Error: Failed to import a required module. Ensure all components exist.")
    print(f"Details: {e}")
    sys.exit(1)

# --- Configuration ---
load_dotenv()
VECTORSTORE_COLLECTION_NAME = os.getenv("VECTORSTORE_COLLECTION_NAME", "google_drive_data")

def log_prompt(prompt_value):
    """A runnable function to print the prompt being sent to the LLM."""
    print("\n--- Prompt Sent to LLM ---")
    # The prompt object can be converted to a string to see the fully formatted version.
    print(prompt_value.to_string())
    print("--------------------------\n")
    return prompt_value

def main():
    """
    Main function to test and evaluate the RAG pipeline.
    """
    print("--- Starting RAG Pipeline Evaluation with Ragas ---")

    # 1. --- Create a Test Dataset ---
    # This includes questions and, for some metrics, ground truth answers based on provided documents.
    test_questions = [
    "What should an employee do immediately after receiving a cash payment from a customer?",
    "How should an employee deposit cash if they are not near a Wells Fargo branch?",
    "Who is responsible if cash is lost while in the custody of a single employee?",
    "What are the requirements for counting cash received in the office?",
    "Can an employee use personal funds to cover a cash shortfall in an emergency?",
    "What is the distributor unit price for MB-1004 (4 oz Premium Ultrafine Mycorrhizae)?",
    "What is the wholesale case total for MB-1003 (3 lb Premium Ultrafine Mycorrhizae)?",
    "What is the MSRP per unit for MG-2002 (2 lb Granular Plus Mycorrhizae)?",
    "How many units are in a case of MG-2015 (15 lb Granular Plus Mycorrhizae)?",
    "What is the distributor case total for MB-1050 (30 lb Premium Ultrafine Mycorrhizae)?"
    ]

    ground_truths = [
        "They should immediately report the cash via email to their supervisor.",
        "They should turn the cash into a cashier's check with the company as the beneficiary and mail it to the home office.",
        "The employee who has custody of the cash will be held responsible for the missing amount.",
        "In-office money must be counted with two employees present.",
        "Yes, in an emergency, an employee can use personal funds and will be reimbursed by the company with proper documentation.",
        "$9.33",
        "$393.96",
        "$39.91",
        "2 units",
        "$292.12"
    ]


    # --- Setup RAG chain (as you already have) ---
    vector_manager = PGVectorManager()
    retriever = vector_manager.get_retriever(
        collection_name=VECTORSTORE_COLLECTION_NAME, async_mode=False, k=3
    )
    template = """
    You are an expert assistant...
    Context:
    {context}
    ---
    Question: {question}
    """
    prompt = ChatPromptTemplate.from_template(template)
    llm = ChatOpenAI(model_name="gpt-4o", temperature=0)
    
    def format_docs(docs):
        formatted_docs = []
        for doc in docs:
            # Start with the main document info
            doc_info = f"Source Document: {doc.metadata.get('file_name', 'N/A')}\nSummary: {doc.page_content}"
            
            # The 'image_data' from metadata is already a list of dicts, so we use it directly.
            image_descriptions = doc.metadata.get('image_data')
            
            if image_descriptions and isinstance(image_descriptions, list):
                doc_info += "\n\nAssociated Images:"
                for img in image_descriptions:
                    # Ensure description and path are strings, providing defaults if they are missing.
                    desc = img.get('description') or "No description provided."
                    path = img.get('image_path') or "N/A"
                    # Create the formatted string for each image with correct newline characters.
                    doc_info += f"\n- Image Path: {path}\n  Description: {desc}"

            formatted_docs.append(doc_info)
        # Use a clear separator with correct newline characters for the LLM.
        return "\n\n---\n\n".join(formatted_docs)

    # This part of the chain takes docs and a question, formats them, and generates an answer
    rag_chain_from_docs = (
        RunnablePassthrough.assign(context=(lambda x: format_docs(x["context"])))
        | prompt
        | llm
        | StrOutputParser()
    )

    # This is the full chain that retrieves, passes docs for the answer, and also returns the source docs
    rag_chain_with_source = RunnableParallel(
        {"context": retriever, "question": RunnablePassthrough()}
    ).assign(answer=rag_chain_from_docs)


    # 2. --- Run RAG Chain on Test Data ---
    print("\nRunning RAG pipeline on test dataset...")
    results = [rag_chain_with_source.invoke(q) for q in test_questions]
    answers = [r["answer"] for r in results]
    contexts = [[doc.page_content for doc in r["context"]] for r in results]

    # 3. --- Evaluate with Ragas ---
    # Create a Hugging Face Dataset from the results
    response_dataset = Dataset.from_dict({
        "question": test_questions,
        "answer": answers,
        "contexts": contexts,
        "ground_truth": ground_truths
    })

    print("\nEvaluating results with Ragas...")
    # Define the metrics you want to calculate
    metrics = [
        faithfulness,
        answer_relevancy,
        context_recall,
        context_precision,
    ]
    
    # Run the evaluation
    result = evaluate(response_dataset, metrics)

    print("\n--- Ragas Evaluation Results ---")
    print(result)
    print("---------------------------------")
    
    # You can also convert to a pandas dataframe for better viewing
    # df = result.to_pandas()
    # print(df)

    vector_manager.close_sync()


if __name__ == "__main__":
    main()
