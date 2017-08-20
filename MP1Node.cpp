/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

// FIXME PREPEND std NAMESPACE!

const int MP1Node::gossipTargets = 2;

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
    for( int i = 0; i < 6; i++ ) {
        NULLADDR[i] = 0;
    }
    this->memberNode = member;
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
        return false;
    }
    else {
        return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();
    
    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }
    
    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }
    
    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
    /*
     * This function is partially implemented and may require changes
     */
    int id = *(int*)(&memberNode->addr.addr);
    int port = *(short*)(&memberNode->addr.addr[4]);
    
    memberNode->bFailed = false;
    memberNode->inited = true;
    memberNode->inGroup = false;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TGOSSIP;
    memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);
    
    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
    MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif
    
    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));
        
        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
        
#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif
        
        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);
        
        free(msg);
    }
    
    return 1;
    
}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
    /*
     * Your code goes here
     */
    memberNode->inGroup = false;
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TGOSSIP;
    memberNode->timeOutCounter = -1;
    memberNode->memberList.clear();
    
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
        return;
    }
    
    // Check my messages
    checkMessages();
    
    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
        return;
    }
    
    // ...then jump in and share your responsibilites!
    nodeLoopOps();
    
    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;
    
    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    /*
     * Your code goes here
     */
    
    bool messageProcessed = true;
    
    MessageHdr receivedMessageHeader;
    memcpy(&receivedMessageHeader, data, sizeof(MessageHdr));
    
    switch (receivedMessageHeader.msgType) {
        case JOINREQ:
            processJoinRequest(data);
            break;
        case JOINREP:
            processJoinResponse(data);
            break;
        case HEARTBEAT:
            processHeartbeat(data);
            break;
        default:
            messageProcessed = false;
            break;
    }
    
    return messageProcessed;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    /*
     * Your code goes here
     */
    
    if (memberNode->pingCounter > 0) {
        memberNode->pingCounter--;
    }
    else {
        memberNode->pingCounter = TGOSSIP;
        memberNode->heartbeat++;
        
        //                		for (auto &memberListEntry : memberNode->memberList) {
        //                			// send heartbeat
        //                			size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + sizeof(long);
        //                			MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
        //                			msg->msgType = HEARTBEAT;
        //
        //                			memcpy((char*)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        //                			size_t memOffset = sizeof(memberNode->addr.addr);
        //                			memcpy((char*)(msg+1) + memOffset, &memberNode->heartbeat, sizeof(long));
        //
        //                			// Send HEARTBEAT message to destination node
        //                            Address memberAddress = getMemberListEntryAddress(&memberListEntry);
        //                			emulNet->ENsend(&memberNode->addr, &memberAddress, (char *)msg, msgsize);
        //
        //                			free(msg);
        //                		}

        std::set<int> randomIndices;
//        int maxRandIndices = min(gossipTargets, static_cast<int>(memberNode->memberList.size()) - 1);
        int maxRandIndices = min(gossipTargets, static_cast<int>(memberNode->memberList.size()) - 1);
        while (static_cast<int>(randomIndices.size()) < maxRandIndices) {
            // randomly get an index
            int randIdx = getRandomInteger(0, memberNode->memberList.size() - 1);
            
//            if (distance(memberNode->memberList.begin() + randIdx, memberNode->myPos) == 0) {
//                continue;
//            }
            randomIndices.insert(randIdx);
        }
        
        // FIXME - duplicated code with JOINREP send message
        size_t msgsize = sizeof(MessageHdr) + sizeof(size_t);
        size_t memOffset = sizeof(size_t); // reset memOffset for use in copying the memberList
        msgsize += memberNode->memberList.size() * (sizeof(int) + sizeof(short) + sizeof(long));
        MessageHdr *msg = (MessageHdr *) malloc(msgsize * sizeof(char));
        
        msg->msgType = HEARTBEAT;
        size_t memberListSize = memberNode->memberList.size();
        memcpy((char *)(msg+1), &memberListSize, sizeof(size_t));
        
        for (auto &memberListEntry: memberNode->memberList) {
            memcpy((char *)(msg+1) + memOffset, &memberListEntry.id, sizeof(int));
            memOffset += sizeof(int);
            
            memcpy((char *)(msg+1) + memOffset, &memberListEntry.port, sizeof(short));
            memOffset += sizeof(short);
            
            memcpy((char *)(msg+1) + memOffset, &memberListEntry.heartbeat, sizeof(long));
            memOffset += sizeof(long);
        }
        
        for (int idx : randomIndices) {
            auto member = memberNode->memberList[idx];
            
            Address memberaddr = getMemberListEntryAddress(&member);
            emulNet->ENsend(&memberNode->addr, &memberaddr, (char *)msg, msgsize);
        }
        
        free(msg);
        
        
        
    }
    
    // FIXME - needs to exclude self (maybe compare *it to myPos?)
    for(auto it = memberNode->memberList.begin(); it != memberNode->memberList.end();) {
        if(memberNode->timeOutCounter - it->gettimestamp() > TREMOVE && it->id != *(int *)(&memberNode->addr.addr)) {
#ifdef DEBUGLOG
            Address memberaddr;
            memset(&memberaddr, 0, sizeof(Address));
            *(int *)(&memberaddr.addr) = it->id;
            *(short *)(&memberaddr.addr[4]) = it->port;
            log->logNodeRemove(&memberNode->addr, &memberaddr);
#endif

            cout << "Node " << it->id << " removed by node " << *(int *)(&memberNode->addr.addr) << " because";
            cout << " my local time is " << memberNode->timeOutCounter << " and the other node's is " << it->timestamp << endl;
            it = memberNode->memberList.erase(it);
        }
        else {
            ++it;
        }
    }
    
    memberNode->timeOutCounter++;
    
    int selfId = *(int *)(&memberNode->addr.addr);
    MemberListEntry *self = getMemberFromMemberList(selfId);
//    cout << "My ID is: " << selfId << " and I'm about to see if I exist:" << endl;
    if (self) {
//        cout << "I do exist! I'm about to update myself in my membership list" << endl;
        self->setheartbeat(memberNode->heartbeat);
        self->settimestamp(memberNode->timeOutCounter);
    }
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
    return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;
    
    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;
    
    return joinaddr;
}

Address MP1Node::getMemberListEntryAddress(MemberListEntry *entry) {
    Address entryaddr;
    
    memset(&entryaddr, 0, sizeof(Address));
    *(int *)(&entryaddr.addr) = entry->id;
    *(short *)(&entryaddr.addr[4]) = entry->port;
    
    return entryaddr;
}

Address MP1Node::getAddressFromIDAndPort(int id, short port) {
    Address entryaddr;
    
    memset(&entryaddr, 0, sizeof(Address));
    *(int *)(&entryaddr.addr) = id;
    *(short *)(&entryaddr.addr[4]) = port;
    
    return entryaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
    memberNode->memberList.clear();
    
    int selfId = *(int *)(&memberNode->addr.addr);
    short selfPort = *(short *)(&memberNode->addr.addr[4]);
    MemberListEntry self(selfId, selfPort, memberNode->heartbeat, memberNode->timeOutCounter);
    memberNode->memberList.push_back(self);
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
           addr->addr[3], *(short*)&addr->addr[4]) ;
}

void MP1Node::processJoinRequest(char *data) {
    // get member list entry info from data
    int id;
    short port;
    long heartbeat;
    size_t memOffset = sizeof(MessageHdr);
    
    memcpy(&id, data + memOffset, sizeof(int));
    memOffset += sizeof(int);
    
    
    memcpy(&port, data + memOffset, sizeof(short));
    memOffset += sizeof(short);
    
    memcpy(&heartbeat, data + memOffset, sizeof(long));
    
    MemberListEntry newEntry(id, port, heartbeat, memberNode->timeOutCounter);
    newEntry.settimestamp(memberNode->timeOutCounter);
    
    // add to your own member list vector
    memberNode->memberList.push_back(newEntry);
    
    Address newEntryAddr = getMemberListEntryAddress(&newEntry);
    
#ifdef DEBUGLOG
    log->logNodeAdd(&memberNode->addr, &newEntryAddr);
#endif
    
    // create JOINREP message: format of data is {struct MessageHdr + memberList.size() + the member list}
    size_t msgsize = sizeof(MessageHdr) + sizeof(size_t);
    memOffset = sizeof(size_t); // reset memOffset for use in copying the memberList
    msgsize += memberNode->memberList.size() * (sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long));
    MessageHdr *msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    
    msg->msgType = JOINREP;
    size_t memberListSize = memberNode->memberList.size();
    memcpy((char *)(msg+1), &memberListSize, sizeof(size_t));
    
    for (auto &memberListEntry: memberNode->memberList) {
        memcpy((char *)(msg+1) + memOffset, &memberListEntry.id, sizeof(int));
        memOffset += sizeof(int);
        
        memcpy((char *)(msg+1) + memOffset, &memberListEntry.port, sizeof(short));
        memOffset += sizeof(short);
        
        memcpy((char *)(msg+1) + memOffset, &memberListEntry.heartbeat, sizeof(long));
        memOffset += sizeof(long);
        
        memcpy((char *)(msg+1) + memOffset, &memberListEntry.timestamp, sizeof(long));
        memOffset += sizeof(long);
    }
    
    // send JOINREP message to new member
    emulNet->ENsend(&memberNode->addr, &newEntryAddr, (char *)msg, msgsize);
    
    free(msg);
}

void MP1Node::processJoinResponse(char *data) {
    // this node is now in the group
    memberNode->inGroup = true;
    
    // "decode" each member list entry sent to you and add to your own member list
    size_t memOffset = sizeof(MessageHdr);
    
    size_t memberListSize;
    memcpy(&memberListSize, data + memOffset, sizeof(size_t));
    
    memOffset += sizeof(size_t);
    
    for (size_t i = 0; i != memberListSize; ++i) {
        int id;
        short port;
        long heartbeat;
        long timestamp;
        
        memcpy(&id, data + memOffset, sizeof(int));
        memOffset += sizeof(int);
        
        memcpy(&port, data + memOffset, sizeof(short));
        memOffset += sizeof(short);
        
        memcpy(&heartbeat, data + memOffset, sizeof(long));
        memOffset += sizeof(long);
        
        memcpy(&timestamp, data + memOffset, sizeof(long));
        memOffset += sizeof(long);
        
        // FIXME - probably don't need this timestamp variable at all, use this node's timeout counter
        MemberListEntry member(id, port, heartbeat, memberNode->timeOutCounter);
        memberNode->memberList.push_back(member);
        
#ifdef DEBUGLOG
        Address memberAddress = getMemberListEntryAddress(&member);
        log->logNodeAdd(&memberNode->addr, &memberAddress);
#endif
    }
}

void MP1Node::processHeartbeat(char *data) {
    // FIXME - getting duplicated code from processJoinResponse
    size_t memOffset = sizeof(MessageHdr);
    size_t memberListSize;
    memcpy(&memberListSize, data + memOffset, sizeof(size_t));
    
    memOffset += sizeof(size_t);
    
    for (size_t i = 0; i != memberListSize; ++i) {
        int id;
        short port;
        long heartbeat;
        
        memcpy(&id, data + memOffset, sizeof(int));
        memOffset += sizeof(int);
        
        memcpy(&port, data + memOffset, sizeof(short));
        memOffset += sizeof(short);
        
        memcpy(&heartbeat, data + memOffset, sizeof(long));
        memOffset += sizeof(long);
        
        MemberListEntry *member = getMemberFromMemberList(id);
        if (member && heartbeat > member->heartbeat) {
            member->setheartbeat(heartbeat);
            member->settimestamp(memberNode->timeOutCounter);
        }
        // or add it if it doesn't
        else if (!member) {
            member = new MemberListEntry(id, port, heartbeat, memberNode->timeOutCounter);
            memberNode->memberList.push_back(*member);
            
#ifdef DEBUGLOG
            Address memberAddress = getMemberListEntryAddress(member);
            log->logNodeAdd(&memberNode->addr, &memberAddress);
#endif
            
            delete member;
        }
    }
    
    
    
    
    
    
    
    
    
    
    
    //        	// "decode" the heartbeat info sent to you
    //        	int id;
    //        	short port;
    //        	long heartbeat;
    //        	size_t memOffset = sizeof(MessageHdr);
    //
    //        	memcpy(&id, data + memOffset, sizeof(int));
    //        	memOffset += sizeof(int);
    //
    //        	memcpy(&port, data + memOffset, sizeof(short));
    //        	memOffset += sizeof(short);
    //
    //        	memcpy(&heartbeat, data + memOffset, sizeof(long));
    //
    //        	// either update the sender node's heartbeat info if it exists in your member list
    //        	MemberListEntry *sender = getMemberFromMemberList(id);
    //        	if (sender) {
    //        		sender->setheartbeat(heartbeat);
    //        		sender->settimestamp(memberNode->timeOutCounter);
    //        	}
    //        	// or add it if it doesn't
    //        	else {
    //        		sender = new MemberListEntry(id, port, heartbeat, memberNode->timeOutCounter);
    //        		memberNode->memberList.push_back(*sender);
    //
    //        #ifdef DEBUGLOG
    //                Address senderAddress = getMemberListEntryAddress(sender);
    //        		log->logNodeAdd(&memberNode->addr, &senderAddress);
    //        #endif
    //
    //        		delete sender;
    //        	}
}

MemberListEntry* MP1Node::getMemberFromMemberList(int id) {
    MemberListEntry *foundEntry = nullptr;
    
    for (auto &memberListEntry : memberNode->memberList) {
        if (memberListEntry.id == id) {
            foundEntry = &memberListEntry;
            break;
        }
    }
    
    return foundEntry;
}

bool MP1Node::areAddressesEqual(Address *a1, Address *a2) {
    return memcmp((char*)&(a1->addr), (char*)&(a2->addr), sizeof(a1->addr)) == 0;
}

int MP1Node::getRandomInteger(int begin, int end) {
    std::random_device rd;  //Will be used to obtain a seed for the random number engine
    std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
    uniform_int_distribution<> dis(begin, end);
    //Use dis to transform the random unsigned int generated by gen into an int in [begin, end]
    return dis(gen);
}
