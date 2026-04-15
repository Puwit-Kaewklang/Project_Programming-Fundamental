import pymysql
import os
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware #Nessessory library for frontend
from fastapi import FastAPI , HTTPException , Request ,Depends
from pydantic import BaseModel
import requests
import json
from sqlalchemy import create_engine , text , Date
from sqlalchemy.orm import sessionmaker , Session

load_dotenv()
job = FastAPI()

#======= To allow Webpage use API =========
job.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_methods=["*"],
    allow_headers=["*"],
)
#===========================================

engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}" ,
    pool_size=10 ,
    max_overflow=20
)

SessionLocal = sessionmaker(bind=engine)
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
        
@job.get("/All-Customer-Data")
def all_customer_data(db: Session = Depends(get_db)):
    try:
        sql = text("""SELECT * FROM customer_detail""")
        result = db.execute(sql)
    except Exception as e:
        raise HTTPException(status_code=500 , detail=str(e))
    return result.mappings().all()

@job.get("/All-Freelance-Data")
def all_customer_data(db: Session = Depends(get_db)):
    try:
        sql = text("""SELECT * FROM freelance_detail""")
        result = db.execute(sql)
    except Exception as e:
        raise HTTPException(status_code=500 , detail=str(e))
    return result.mappings().all()

@job.get("/Job-List")
def job_list(db: Session = Depends(get_db)):
    try:
        sql = text("""SELECT j.Job_id , j.Job_Name , j.Posted_On , d.Budget , jt.Type_Of_Job , s.Status_Of_Job 
                   FROM job as j inner join status as s inner join job_type as jt inner join detail as d
                   ON j.Status_id = s.Status_id and j.Type_id = jt.Type_id and j.Job_id = d.Job_id""")
        result = db.execute(sql)
    except Exception as e:
        raise HTTPException(status_code=500 , detail=str(e))
    return result.mappings().all()

@job.get("/Ranking-Freelance")
def job_analytic(db: Session = Depends(get_db)):
    try:
        sql = text("""select f.Freelance_id , f.Freelance_FirstName , f.Freelance_LastName , count(j.Status_id) as Total_Completed_Job
                    from freelance_detail as f inner join job_select as js inner join job as j inner join status as s
                    on f.Freelance_id = js.Freelance_id and js.Job_id = j.Job_id and j.Status_id = s.Status_id and s.Status_id = 'S3'
                    group by f.Freelance_id order by count(j.Status_id) desc;""")
        result = db.execute(sql)
    except Exception as e:
        raise HTTPException(status_code=400 , detail=str(e))
    
    return result.mappings().all()

@job.get("/Job-Analytic")
def job_analytic(db: Session = Depends(get_db)):
    try:
        sql = text("""select jt.Type_Of_Job , count(j.Job_id) as Total_Job , sum(d.Budget) as Total_Budget 
                    from job.job_type as jt inner join job.job as j inner join job.detail as d
                    on	jt.Type_id = j.Type_id and j.Job_id = d.Job_id
                    group by j.Type_id;""")
        result = db.execute(sql)
    except Exception as e:
        raise HTTPException(status_code=400 , detail=str(e))
    return result.mappings().all()

class upload_job(BaseModel):
    Job_id: str
    Job_Name: str
    Posted_On: str = Date
    Deadline: str = Date
    Customer_id: str
    Status: str
    Type: str
    Brief: str
    Budget: float
    Requirement_Skill: str
    
@job.post("/Post-Job")
def post_job(data: upload_job , db: Session = Depends(get_db)):
    try:
        status = text("""SELECT Status_id FROM status WHERE Status_Of_Job = :sj""")
        result_status = db.execute(status , {"sj": data.Status}).fetchone()
        
        type = text("""SELECT Type_id FROM job_type WHERE Type_Of_Job = :tj""")
        result_type = db.execute(type , {"tj":data.Type}).fetchone()
        
        job = text("""INSERT INTO job VALUES (:jid , :jn , :p , :sid , :cid , :tid)""")
        db.execute(job , {"jid":data.Job_id , "jn":data.Job_Name , "p":data.Posted_On , "sid":result_status[0] , "cid":data.Customer_id , "tid":result_type[0]})
        
        detail = text("""INSERT INTO detail VALUES (:djid , :dl , :b , :bg , :rs)""")
        db.execute(detail , {"djid":data.Job_id , "dl":data.Deadline , "b":data.Brief , "bg":data.Budget , "rs":data.Requirement_Skill})
        db.commit()
    except Exception as e:
        raise HTTPException(status_code=400 , detail=str(e))
    
    return f"Done Uploading"