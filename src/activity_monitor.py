#!/usr/bin/env python3
"""
FCB ACTIVITY MONITOR - REAL-TIME PUMP/DUMP DETECTION
Scans database whales every 60 seconds for coordination patterns
Detects pump/dump schemes and whale movement correlation
Deploy as separate Render Background Worker
"""

import os
import requests
import psycopg
import json
import time
import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from collections import defaultdict
import statistics

print("⚡ FCB ACTIVITY MONITOR STARTING - PUMP/DUMP DETECTION...", flush=True)
print(f"📊 Environment check - DB_URL exists: {bool(os.getenv('DB_URL'))}", flush=True)

# Database configuration
DATABASE_URL = os.getenv('DB_URL', "postgresql://wallet_admin:AbRD14errRCD6H793FRCcPvXIRLgNugK@dpg-d1vd05je5dus739m8mv0-a.frankfurt-postgres.render.com:5432/wallet_transactions")

# API Keys
ETHERSCAN_API_KEY = os.getenv('ETHERSCAN_API_KEY', 'GCB4J11T34YG29GNJJX7R7JADRTAFJKPDE')
COINGECKO_PRO_API_KEY = os.getenv('COINGECKO_API_KEY', 'CG-bJP1bqyMemFNQv5dp4nvA9xm')

# Activity Monitor Configuration
SCAN_INTERVAL = 60  # Scan every 60 seconds
COORDINATION_THRESHOLD = 3  # 3+ whales acting together = potential coordination
TIME_WINDOW_MINUTES = 15  # Look for activity within 15-minute windows
VOLUME_CORRELATION_THRESHOLD = 0.8  # 80%+ volume correlation = suspicious
WHALE_THRESHOLD = 1000  # $1000+ to be considered whale activity

# Pump/Dump Detection Parameters
PUMP_INDICATORS = {
    'rapid_buys': 5,  # 5+ whales buying same token in 15 minutes
    'volume_spike': 3.0,  # 3x normal volume
    'price_increase': 0.15  # 15%+ price increase
}

DUMP_INDICATORS = {
    'rapid_sells': 4,  # 4+ whales selling same token in 15 minutes
    'volume_spike': 2.5,  # 2.5x normal volume  
    'price_decrease': 0.10  # 10%+ price decrease
}

class ActivityMonitor:
    def __init__(self):
        self.scan_count = 0
        self.total_alerts = 0
        self.whale_activity_history = defaultdict(list)
        self.token_price_history = defaultdict(list)
        
    def get_database_connection(self):
        """Get database connection"""
        try:
            return psycopg.connect(DATABASE_URL)
        except Exception as e:
            print(f"❌ Database connection failed: {e}", flush=True)
            return None
    
    def get_database_whales(self):
        """Get all whale addresses from database"""
        conn = self.get_database_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT wallet_address, coin_symbol, 
                       COUNT(*) as transaction_count,
                       SUM(amount_usd) as total_volume,
                       MAX(block_timestamp) as last_activity
                FROM whale_transactions 
                WHERE block_timestamp > NOW() - INTERVAL '7 days'
                GROUP BY wallet_address, coin_symbol
                HAVING SUM(amount_usd) > %s
                ORDER BY total_volume DESC
            """, (WHALE_THRESHOLD,))
            
            whales = cursor.fetchall()
            return whales
            
        except Exception as e:
            print(f"⚠️ Failed to get database whales: {e}", flush=True)
            return []
        finally:
            conn.close()
    
    def get_recent_whale_activity(self, time_window_minutes=15):
        """Get whale activity in the last X minutes"""
        conn = self.get_database_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=time_window_minutes)
            
            cursor.execute("""
                SELECT wallet_address, coin_symbol, activity_type,
                       amount_tokens, amount_usd, block_timestamp,
                       transaction_id
                FROM whale_transactions 
                WHERE block_timestamp > %s
                AND amount_usd > %s
                ORDER BY block_timestamp DESC
            """, (cutoff_time, WHALE_THRESHOLD))
            
            activities = cursor.fetchall()
            return activities
            
        except Exception as e:
            print(f"⚠️ Failed to get recent activity: {e}", flush=True)
            return []
        finally:
            conn.close()
    
    def get_token_price(self, token_symbol):
        """Get current token price from database or API"""
        # For now, return mock price - in production, integrate with price API
        mock_prices = {
            'UNI': 12.45, 'LINK': 18.20, 'AAVE': 95.30, 'COMP': 75.15,
            'CRV': 0.85, 'SUSHI': 2.15, 'PEPE': 0.00002, 'SHIB': 0.000025,
            'FLOKI': 0.00015, 'USDC': 1.00, 'USDT': 1.00, 'DAI': 1.00,
            'APE': 3.20, 'SAND': 0.45, 'MANA': 0.65, 'MATIC': 0.85, 'ARB': 1.25
        }
        return mock_prices.get(token_symbol, 1.0)
    
    def detect_coordination_patterns(self, activities):
        """Detect coordination patterns among whale activities"""
        if not activities:
            return []
        
        # Group activities by token and time
        token_activities = defaultdict(lambda: defaultdict(list))
        
        for activity in activities:
            wallet, token, activity_type, tokens, usd, timestamp, tx_id = activity
            time_bucket = timestamp.replace(second=0, microsecond=0)  # Group by minute
            
            token_activities[token][time_bucket].append({
                'wallet': wallet,
                'type': activity_type,
                'usd_amount': float(usd),
                'timestamp': timestamp,
                'tx_id': tx_id
            })
        
        coordination_alerts = []
        
        # Analyze each token for coordination patterns
        for token, time_buckets in token_activities.items():
            for time_bucket, activities_in_bucket in time_buckets.items():
                if len(activities_in_bucket) >= COORDINATION_THRESHOLD:
                    
                    # Analyze activity types
                    buy_activities = [a for a in activities_in_bucket if a['type'] in ['buy', 'transfer']]
                    sell_activities = [a for a in activities_in_bucket if a['type'] == 'sell']
                    
                    # Check for pump pattern (coordinated buying)
                    if len(buy_activities) >= PUMP_INDICATORS['rapid_buys']:
                        total_volume = sum(a['usd_amount'] for a in buy_activities)
                        unique_wallets = len(set(a['wallet'] for a in buy_activities))
                        
                        coordination_alerts.append({
                            'type': 'POTENTIAL_PUMP',
                            'token': token,
                            'time': time_bucket,
                            'whale_count': unique_wallets,
                            'total_volume': total_volume,
                            'activities': buy_activities,
                            'confidence': min(unique_wallets / PUMP_INDICATORS['rapid_buys'], 1.0)
                        })
                    
                    # Check for dump pattern (coordinated selling)
                    if len(sell_activities) >= DUMP_INDICATORS['rapid_sells']:
                        total_volume = sum(a['usd_amount'] for a in sell_activities)
                        unique_wallets = len(set(a['wallet'] for a in sell_activities))
                        
                        coordination_alerts.append({
                            'type': 'POTENTIAL_DUMP',
                            'token': token,
                            'time': time_bucket,
                            'whale_count': unique_wallets,
                            'total_volume': total_volume,
                            'activities': sell_activities,
                            'confidence': min(unique_wallets / DUMP_INDICATORS['rapid_sells'], 1.0)
                        })
        
        return coordination_alerts
    
    def analyze_wallet_correlations(self, activities):
        """Analyze wallet behavior correlations"""
        if len(activities) < 2:
            return []
        
        # Group activities by wallet
        wallet_activities = defaultdict(list)
        for activity in activities:
            wallet = activity[0]
            wallet_activities[wallet].append(activity)
        
        correlations = []
        wallet_list = list(wallet_activities.keys())
        
        # Compare each pair of wallets
        for i in range(len(wallet_list)):
            for j in range(i + 1, len(wallet_list)):
                wallet_a = wallet_list[i]
                wallet_b = wallet_list[j]
                
                activities_a = wallet_activities[wallet_a]
                activities_b = wallet_activities[wallet_b]
                
                # Find common tokens
                tokens_a = set(activity[1] for activity in activities_a)
                tokens_b = set(activity[1] for activity in activities_b)
                common_tokens = tokens_a.intersection(tokens_b)
                
                if len(common_tokens) >= 2:  # Trading same tokens
                    # Check timing correlation
                    times_a = [activity[5] for activity in activities_a if activity[1] in common_tokens]
                    times_b = [activity[5] for activity in activities_b if activity[1] in common_tokens]
                    
                    # Calculate time correlation (activities within 5 minutes)
                    time_correlations = 0
                    for time_a in times_a:
                        for time_b in times_b:
                            if abs((time_a - time_b).total_seconds()) <= 300:  # 5 minutes
                                time_correlations += 1
                    
                    if time_correlations >= 2:
                        correlation_score = min(time_correlations / max(len(times_a), len(times_b)), 1.0)
                        
                        correlations.append({
                            'wallet_a': wallet_a,
                            'wallet_b': wallet_b,
                            'common_tokens': list(common_tokens),
                            'time_correlations': time_correlations,
                            'correlation_score': correlation_score,
                            'suspicion_level': 'HIGH' if correlation_score > 0.7 else 'MEDIUM'
                        })
        
        return correlations
    
    def save_alert_to_database(self, alert_data):
        """Save pump/dump alert to database"""
        conn = self.get_database_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            # Create alerts table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pump_dump_alerts (
                    id SERIAL PRIMARY KEY,
                    alert_type VARCHAR(50) NOT NULL,
                    token_symbol VARCHAR(20) NOT NULL,
                    whale_count INTEGER NOT NULL,
                    total_volume DECIMAL(20,2) NOT NULL,
                    confidence_score DECIMAL(3,2) NOT NULL,
                    alert_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    alert_data JSONB,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)
            
            cursor.execute("""
                INSERT INTO pump_dump_alerts 
                (alert_type, token_symbol, whale_count, total_volume, confidence_score, alert_data)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                alert_data['type'],
                alert_data['token'],
                alert_data['whale_count'],
                alert_data['total_volume'],
                alert_data['confidence'],
                json.dumps(alert_data, default=str)
            ))
            
            conn.commit()
            return True
            
        except Exception as e:
            print(f"⚠️ Failed to save alert: {e}", flush=True)
            return False
        finally:
            conn.close()
    
    def monitor_whale_activity(self):
        """Main monitoring function - scan for pump/dump patterns"""
        print(f"\n⚡ ACTIVITY SCAN #{self.scan_count + 1} - PUMP/DUMP DETECTION", flush=True)
        print(f"🕐 {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')} - Real-time whale monitoring", flush=True)
        print("─" * 60, flush=True)
        
        # Get recent whale activity
        recent_activities = self.get_recent_whale_activity(TIME_WINDOW_MINUTES)
        
        if not recent_activities:
            print("😴 No recent whale activity detected", flush=True)
            return 0
        
        print(f"📊 Found {len(recent_activities)} whale activities in last {TIME_WINDOW_MINUTES} minutes", flush=True)
        
        # Group activities by token for analysis
        token_summary = defaultdict(lambda: {'buys': 0, 'sells': 0, 'volume': 0, 'whales': set()})
        
        for activity in recent_activities:
            wallet, token, activity_type, tokens, usd, timestamp, tx_id = activity
            token_summary[token]['whales'].add(wallet)
            token_summary[token]['volume'] += float(usd)
            
            if activity_type in ['buy', 'transfer']:
                token_summary[token]['buys'] += 1
            elif activity_type == 'sell':
                token_summary[token]['sells'] += 1
        
        # Display activity summary
        print(f"\n📈 WHALE ACTIVITY SUMMARY:", flush=True)
        for token, summary in token_summary.items():
            whale_count = len(summary['whales'])
            print(f"🪙 {token}: {whale_count} whales, {summary['buys']} buys, {summary['sells']} sells, ${summary['volume']:,.0f}", flush=True)
        
        # Detect coordination patterns
        coordination_alerts = self.detect_coordination_patterns(recent_activities)
        wallet_correlations = self.analyze_wallet_correlations(recent_activities)
        
        alerts_generated = 0
        
        # Process coordination alerts
        if coordination_alerts:
            print(f"\n🚨 COORDINATION ALERTS DETECTED:", flush=True)
            for alert in coordination_alerts:
                print(f"🔥 {alert['type']}: {alert['token']}", flush=True)
                print(f"   🐋 {alert['whale_count']} whales, ${alert['total_volume']:,.0f} volume", flush=True)
                print(f"   📊 Confidence: {alert['confidence']:.1%}", flush=True)
                print(f"   🕐 Time: {alert['time'].strftime('%H:%M:%S UTC')}", flush=True)
                
                # Save alert to database
                if self.save_alert_to_database(alert):
                    alerts_generated += 1
                else:
                    print(f"⚠️ Failed to save alert: Object of type datetime is not JSON serializable", flush=True)
        
        # Process wallet correlations
        if wallet_correlations:
            print(f"\n🕵️ WALLET CORRELATIONS FOUND:", flush=True)
            for correlation in wallet_correlations:
                print(f"🔗 {correlation['suspicion_level']} correlation:", flush=True)
                print(f"   👤 {correlation['wallet_a'][:10]}... ↔️ {correlation['wallet_b'][:10]}...", flush=True)
                print(f"   🪙 Common tokens: {', '.join(correlation['common_tokens'])}", flush=True)
                print(f"   📊 Score: {correlation['correlation_score']:.1%}", flush=True)
        
        if not coordination_alerts and not wallet_correlations:
            print("✅ No suspicious coordination patterns detected", flush=True)
        
        self.total_alerts += alerts_generated
        return alerts_generated
    
    async def run_monitoring_loop(self):
        """Main monitoring loop - continuous 60-second scanning"""
        print("⚡ FCB ACTIVITY MONITOR - REAL-TIME PUMP/DUMP DETECTION", flush=True)
        print("=" * 80, flush=True)
        print(f"🔍 Scanning database whales every {SCAN_INTERVAL} seconds", flush=True)
        print(f"🎯 Coordination threshold: {COORDINATION_THRESHOLD}+ whales", flush=True)
        print(f"⏰ Time window: {TIME_WINDOW_MINUTES} minutes", flush=True)
        print(f"🚨 Pump detection: {PUMP_INDICATORS['rapid_buys']}+ rapid buys", flush=True)
        print(f"📉 Dump detection: {DUMP_INDICATORS['rapid_sells']}+ rapid sells", flush=True)
        print(f"💰 Minimum whale threshold: ${WHALE_THRESHOLD:,}", flush=True)
        
        # Get initial whale count
        database_whales = self.get_database_whales()
        print(f"🐋 Monitoring {len(database_whales)} database whales for coordination", flush=True)
        
        while True:
            try:
                self.scan_count += 1
                
                # Monitor whale activity
                alerts = self.monitor_whale_activity()
                
                # Summary
                print(f"\n📊 SCAN #{self.scan_count} COMPLETE:", flush=True)
                print(f"🚨 Alerts this scan: {alerts}", flush=True)
                print(f"📈 Total alerts generated: {self.total_alerts}", flush=True)
                print(f"⏰ Next scan in {SCAN_INTERVAL} seconds...", flush=True)
                print("=" * 60, flush=True)
                
                # Wait for next scan
                await asyncio.sleep(SCAN_INTERVAL)
                
            except KeyboardInterrupt:
                print("\n🛑 Activity monitor stopped by user", flush=True)
                break
            except Exception as e:
                print(f"⚠️ Monitoring error: {e}", flush=True)
                print(f"🔄 Retrying in {SCAN_INTERVAL} seconds...", flush=True)
                await asyncio.sleep(SCAN_INTERVAL)

def main():
    """Main execution"""
    monitor = ActivityMonitor()
    
    try:
        # Run the monitoring loop
        asyncio.run(monitor.run_monitoring_loop())
    except KeyboardInterrupt:
        print("\n👋 FCB Activity Monitor stopped", flush=True)

if __name__ == "__main__":
    main()
