from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
# from data_storage.snapshots import SnapshotManager

def create_snapshot_job(snapshot_manager):
    """Job to create a new snapshot of exchange data."""
    print(f"Creating snapshot at {datetime.now()}")
    snapshot = snapshot_manager.create_snapshot()
    print(f"Created snapshot {snapshot.id} with {len(snapshot.price_points)} price points")

def setup_scheduler(snapshot_manager):
    """Set up the scheduler with periodic jobs."""
    scheduler = BackgroundScheduler()
    
    # Schedule snapshot creation every 5 minutes
    scheduler.add_job(
        lambda: create_snapshot_job(snapshot_manager), 
        'interval', 
        minutes=5
    )
    
    return scheduler