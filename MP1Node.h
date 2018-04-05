/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"

/**
 * Macros
 */
#define TREMOVE 20
#define TFAIL 10
#define TGOSSIP 5
#define TARGETMEMBERS 2
#define BUFFERTHRESHOLD 10

/**
 * Message Types
 */
enum MsgTypes{
    JOINREQ,
    JOINREP,
    HEARTBEAT,
    PULLREQUEST,
    
    // SWIM message types
    PING,
    ACK,
    PINGTOSURROGATE,
    PINGFROMSURROGATE,
    ACKTOSURROGATE,
    ACKFROMSURROGATE
};

/**
 * Failure Detection Protocol type
 */
enum FailureDetectionProtocol {
    ALLTOALL,
    PUSHGOSSIP,
    PULLGOSSIP,
    SWIM
};

/**
 * STRUCT NAME: MembershipUpdate
 *
 * DESCRIPTION: Keeps track of how many times information about a node was disseminated
 */
struct MembershipUpdate {
    size_t count;
    MemberListEntry node;
};

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
    enum MsgTypes msgType;
}MessageHdr;

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
private:
    EmulNet *emulNet;
    Log *log;
    Member *memberNode;
    
    // Set to true when membership list is updated, false otherwise
    bool hasUpdatesToGive;
    
    // Map to keep track of pinged nodes when using SWIM protocol
    std::unordered_map<int, int> pingedNodes;
    
    // Buffers to keep track of membership updates and how many times they have been disseminated
    std::vector<MembershipUpdate> joinedNodeBuffer;
    std::vector<MembershipUpdate> failedNodeBuffer;
    
    // The type of failure detection protocol to use
    static const FailureDetectionProtocol protocol;
    
    inline int getSelfId() {
        return *(int *)(&memberNode->addr.addr);
    }
    
    inline short getSelfPort() {
        return *(short *)(&memberNode->addr.addr[4]);
    }
    
    // Methods for processing messages
    void processJoinRequest(char *data);
    void updateMembershipList(char *data);
    void processPullRequest(char *data);
    void processPing(char *data);
    void processAck(char *data);
    void processPingToSurrogate(char *data);
    void processPingFromSurrogate(char *data);
    void processAckToSurrogate(char *data);
    void processAckFromSurrogate(char *data);
    
    // Methods for sending membership info
    void allToAllBroadcast();
    void pushGossipBroadcast();
    void pullGossipBroadcast();
    void pingRandomNode();
    
    // TODO - comment this
    void updateNodeInMembershipList(int nodeId, short port, long heartbeat, bool updateBuffer);
    
    // TODO - comment these
    void cleanBuffer(std::vector<MembershipUpdate>& buffer);
    void incrementBufferCounts(std::vector<MembershipUpdate>& buffer);
    void addNodeToBuffer(std::vector<MembershipUpdate>& buffer, MemberListEntry& node);
    void addBufferInfoToMessage(MessageHdr **msg, size_t offset);
    
    // Methods for tracking failed vs. healthy members
    size_t removeFailedMembers();
    size_t getNumberOfHealthyMembers();
    size_t createHealthyMembershipListMsg(MessageHdr **msg, MsgTypes msgType);
    
    // ID and Address methods
    Address getMemberListEntryAddress(MemberListEntry *entry);
    Address getAddressFromIDAndPort(int id, short port);
    MemberListEntry* getMemberFromMemberList(int id);
    
    // Utility method
    // FIXME - this method probably doesn't belong here
    int getRandomInteger(int begin, int end);
    
public:
    MP1Node(Member*, EmulNet*, Log*, Address*);
    inline Member* getMemberNode() {
        return memberNode;
    }
    int recvLoop();
    static int enqueueWrapper(void *env, char *buff, int size);
    void nodeStart(char *servaddrstr, short serverport);
    int initThisNode(Address *joinaddr);
    int introduceSelfToGroup(Address *joinAddress);
    int finishUpThisNode();
    void nodeLoop();
    void checkMessages();
    bool recvCallBack(void *env, char *data, int size);
    void nodeLoopOps();
    Address getJoinAddress();
    void initMemberListTable();
    virtual ~MP1Node();
};

#endif /* _MP1NODE_H_ */
