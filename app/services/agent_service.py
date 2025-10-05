import os
import sys
from dotenv import load_dotenv
from langchain_core.documents import Document
from typing import List
from pydantic import BaseModel, Field

# --- Setup System Path ---
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.append(project_root)

# --- Project-specific Imports ---
from app.database.pg_vector import PGVectorManager
from langchain.tools import tool
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.messages import HumanMessage, AIMessage

# --- Configuration ---
load_dotenv()
VECTORSTORE_COLLECTION_NAME = os.getenv("VECTORSTORE_COLLECTION_NAME", "google_drive_data")

# --- Document System Specific Information ---
COMPANY_PHILOSOPHY = """
Our business philosophy is simple: Provide a product that is easy to use and that creates amazing results. We strive to sell the best organic and sustainable, all-in-one soil additives for the home gardener & plant enthusiasts."""

GLOSSARY = """
### Product SKU abbreviation:
- ES- Earthshine
- MB- Ultrafine Mycorrhizae
- MG- Granular Mycorrhizae
- FH- Flower Finisher
- FS- Flower Shower
- PLD- Defense
- GGWF- Water Filter
- FV- Pride Lands Veg
- FB- Pride Lands Bloom
- BX- Nature's Brix
- GA- Green Aminos
- HHV- Hybrid Veg
- HHB- Hybrid Bloom
- BMP- Biomend Plu
- BPB- Biophos
- APF_Soil- All Purpose Potting Soil
- FV_Soil- Pride Lands Premium Potting Soil
- C.Top-GA- Green Aminos Countertop display
- C.Top-UF- Ultrafine Countertop display

### General Terms:
- QB- Quickbooks
- LTL- Less than trailer load
- LCL- Less than container load
- FTL- Full trailer load
- FCL- Full Container load
- CFS- Container Freight Station
- FOB- Free on Board
- FOB Ship Point- Buyer takes ownership (pays freight) at the shipping point
- FOB Ex Factory- Buyer takes ownership at the factory where the product is produced
- FOB Destination- Buyer takes ownership once it arrives at their facility (seller pays freight)
- EXW- Ex Works
- CIF- Cost, Insurance, Freight
- DAP- Delivered at place
- DDP- Deliver Duty paid
- FCA- Free Carrier
- Drayage- Shipping from Port to your facility
- Commercial Invoice- International document when shipping from a foreign country.
- Drum- 250 lb. at GG
- Tote- comes in 1000 and 2000 pounds
- Wholesaler- Retail outlet selling directly to the eventual consumer
- Distributor- Acts as middleman between the selling company and the wholesale company.
- MSRP- Manufacturer Suggested Retail Price
"""

# --- Pydantic Models for Structured Output ---
class RelevantFile(BaseModel):
    """A single relevant file with its ID and name."""
    file_id: str = Field(..., description="The unique identifier for the file.")
    file_name: str = Field(..., description="The name of the file.")

class RelevantFiles(BaseModel):
    """A list of the most relevant files."""
    files: List[RelevantFile] = Field(..., description="A list of relevant file objects.")

class DocumentAgent:
    """
    An agentic chatbot for document processing that can perform semantic search
    on internal documents to answer user queries.
    """
    def __init__(self):
        """
        Initializes the agent, its tools, and the underlying LLM.
        """
        self.vector_manager = PGVectorManager()
        self.agent_executor = self._create_agent_executor()

    def _get_docs_from_relevant_files(self, search_term: str) -> list[Document]:
        """
        Identifies the most relevant files using an LLM and retrieves all documents from them.
        """
        all_files = self.vector_manager.get_all_file_metadata()
        if not all_files:
            return []

        file_list_str = "\n".join([f"- {f['file_name']} (ID: {f['file_id']})" for f in all_files])
        llm = ChatOpenAI(model_name="gpt-4o", temperature=0)
        structured_llm = llm.with_structured_output(RelevantFiles)

        prompt = f"""
        You are an expert file system search assistant. Based on the user's query, your task is to identify the top 5 most relevant files from the list below.
        USER QUERY: "{search_term}"
        FILE LIST:
        {file_list_str}
        Your output should be a list of the most relevant files based on the query.
        """
        try:
            response = structured_llm.invoke(prompt)
            relevant_file_ids = [f.file_id for f in response.files]
            if not relevant_file_ids:
                return []
            return self.vector_manager.get_documents_by_file_ids(VECTORSTORE_COLLECTION_NAME, relevant_file_ids)
        except Exception as e:
            print(f"Error processing LLM response for file selection: {e}")
            return []

    def semantic_search(self, search_term: str):
        """
        This tool performs a comprehensive search on internal documents available on google drive and returns the most relevant information.

        For example:
        If the user asks "In what sizes is the Tomato, veggie fertalizer available?" the search term should be "Tomato, veggie fertalizer sizes".
        """
        file_level_docs = self._get_docs_from_relevant_files(search_term)
        retriever = self.vector_manager.get_retriever(
            collection_name=VECTORSTORE_COLLECTION_NAME, async_mode=False, k=13
        )
        semantic_docs = retriever.invoke(search_term)

        combined_docs = file_level_docs + semantic_docs
        unique_docs = {
            (doc.metadata.get('file_id', 'N/A'), doc.page_content): doc
            for doc in combined_docs
        }
        final_docs = list(unique_docs.values())

        formatted_docs = []
        for doc in final_docs:
            doc_info = f"Source Document: {doc.metadata.get('file_name', 'N/A')}\nSummary: {doc.page_content}"
            image_descriptions = doc.metadata.get('image_data')
            if image_descriptions and isinstance(image_descriptions, list):
                # Only include the first 15 images
                doc_info += "\n\nAssociated Images:"
                for img in image_descriptions[:15]:
                    desc = img.get('description') or "No description provided."
                    path = img.get('image_path') or "N/A"
                    doc_info += f"\n- Image Path: {path}\n  Description: {desc}"
            formatted_docs.append(doc_info)
        
        if not formatted_docs:
            return "No relevant information found."
            
        return "\n\n---\n\n".join(formatted_docs)

    def _create_agent_executor(self):
        """
        Creates and returns the LangChain agent executor.
        """
        # The semantic_search method needs to be wrapped with the @tool decorator
        # To do this dynamically for a class method, we create the tool instance manually.
        search_tool = tool(self.semantic_search)
        
        tools = [search_tool]
        llm = ChatOpenAI(model_name="gpt-4o", temperature=0.1)

        system_prompt = f'''
           You are a helpful and friendly conversational assistant for company employees. You have access to a `semantic_search` tool for searching the company's internal documents which are available on google drive. The documents are related to the company's products, business philosophy, SOPS, and internal processes.
           Here is the company's philosophy, which should guide your responses:
            ---
            {COMPANY_PHILOSOPHY}
            ---
            
            Here is a glossary of terms specific to the company.
            ---
            {GLOSSARY}
            ---

            **RESPONSE INSTRUCTIONS:**
            1. You will be given an email from a company employee.
            2. You will have to identify and extract the question from that email by ignoring the email signature and format it as a query term for the `semantic_search` tool.
            3. Use the `semantic_search` tool by passing the query term to find the most relevant documents for that query. For example, if the user asks "In what sizes is the Tomato, veggie fertalizer available?" the search term should be "Tomato, veggie fertalizer sizes".
            4. Based on the retreieved documents and their information, you have to formulate a detailed and comprehensive answer to the question. You should use all of the relevant information from the documents to answer the question. If the document contains images, **you MUST display that image directly in your response** using markdown: `![Description](Image Path)`.
            5. If multiple images are relevant, display all of them.
            6. If you could not find any relevant information, say "I couldn't find any relevant information to answer that question."
            7. **IMPORTANT**: You are strictly forbidden from sharing or mentioning the percentages of any ingredients in your responses. You may list ingredients, but you must NEVER reveal or estimate their percentages under any circumstances.
            8. If you see any percentages in the retreived documents, you should remove them in your response.
        '''

        prompt = ChatPromptTemplate.from_messages([
            ("system", system_prompt),
            MessagesPlaceholder(variable_name="chat_history"),
            ("user", "{input}"),
            MessagesPlaceholder(variable_name="agent_scratchpad"),
        ])

        agent = create_tool_calling_agent(llm, tools, prompt)
        return AgentExecutor(agent=agent, tools=tools, verbose=True, return_intermediate_steps=True)

    def generate_response(self, query: str):
        """
        Generates a response to a user query. This agent is stateless and does not retain chat history.
        """
        try:
            # The agent is stateless, so chat_history is always empty.
            response = self.agent_executor.invoke({
                "input": query,
                "chat_history": []
            })
            return response
        except Exception as e:
            print(f"Agent failed to generate a response: {e}")
            return {
                "output": "Sorry, I encountered an error while processing your request. Please try again.",
                "intermediate_steps": []
            }

if __name__ == '__main__':
    # Example usage of the agent
    print("Initializing Document Agent...")
agent = DocumentAgent()
    
    print("\n--- Testing Agent ---")
    test_query = "How to create a customer in QB?"
    print(f"Query: {test_query}")
    
    response = agent.generate_response(test_query)
    
    print("\n--- Agent Output ---")
    print(response.get('output'))

    print("\n--- Intermediate Steps (Tool Usage) ---")
    if response.get('intermediate_steps'):
        for step in response['intermediate_steps']:
            print(step)
    else:
        print("No tools were used for this query.")